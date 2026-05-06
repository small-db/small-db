// Copyright 2025 Xiaochen Cui
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// =====================================================================
// c++ std
// =====================================================================

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

// pg_query
#include "pg_query.h"
#include "pg_query.pb-c.h"

// absl
#include "absl/status/statusor.h"

// arrow
#include "arrow/api.h"

// spdlog
#include "spdlog/spdlog.h"

// nlohmann/json
#include "nlohmann/json.hpp"

// grpc
#include "grpcpp/create_channel.h"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/catalog/catalog.h"
#include "src/execution/execution.grpc.pb.h"
#include "src/execution/execution.pb.h"
#include "src/gossip/gossip.h"
#include "src/lock/lock_manager.h"
#include "src/rocks/rocks.h"
#include "src/schema/const.h"
#include "src/schema/schema.h"
#include "src/semantics/extract.h"
#include "src/txn/txn.h"
#include "src/type/type.h"

// =====================================================================
// self header
// =====================================================================

#include "src/execution/update.h"

namespace small::execution {

// Pull the WHERE primary-key value out of an UPDATE AST. Only
// `WHERE <pk_col> = <literal>` is supported; anything else (predicate
// WHERE, non-pk column) is rejected because the lock manager and intent
// path are scoped to single rows.
static absl::StatusOr<std::string> extract_pk(
    PgQuery__UpdateStmt* update_stmt,
    const std::shared_ptr<small::schema::Table>& table) {
    std::string filter_column =
        update_stmt->where_clause->a_expr->lexpr->column_ref->fields[0]
            ->string->sval;
    int pk_index = small::schema::get_pk_index(*table);
    if (pk_index < 0 || table->columns()[pk_index].name() != filter_column) {
        return absl::UnimplementedError(fmt::format(
            "UPDATE WHERE column must be the primary key (got '{}'); "
            "multi-row UPDATE is not supported yet",
            filter_column));
    }
    auto filter_value = small::semantics::extract_const(
                            update_stmt->where_clause->a_expr->rexpr->a_const)
                            .value();
    return small::type::encode(filter_value);
}

// Apply the SET clause to a single row's column map and produce the
// resulting (column-order) values vector ready for an intent write.
static absl::StatusOr<std::vector<std::string>> apply_set_clause(
    PgQuery__UpdateStmt* update_stmt,
    const std::shared_ptr<small::schema::Table>& table,
    std::map<std::string, std::string> updated) {
    for (size_t i = 0; i < update_stmt->n_target_list; i++) {
        auto res_target = update_stmt->target_list[i]->res_target;
        auto column_name = std::string(res_target->name);
        auto val_node = res_target->val;

        std::string new_encoded_value;

        if (val_node->node_case == PG_QUERY__NODE__NODE_A_EXPR) {
            auto expr = val_node->a_expr;
            auto op = std::string(expr->name[0]->string->sval);

            auto ref_column = std::string(
                expr->lexpr->column_ref->fields[0]->string->sval);
            auto current_encoded = updated.at(ref_column);

            small::type::Type col_type = small::type::Type::STRING;
            for (const auto& col : table->columns()) {
                if (col.name() == column_name) {
                    col_type = col.type();
                    break;
                }
            }

            auto current_datum =
                small::type::decode(current_encoded, col_type);
            auto const_datum =
                small::semantics::extract_const(expr->rexpr->a_const).value();

            if (col_type == small::type::Type::INT64) {
                int64_t current_val = current_datum.int64_value();
                int64_t const_val = const_datum.int64_value();
                int64_t result;
                if (op == "-") {
                    result = current_val - const_val;
                } else if (op == "+") {
                    result = current_val + const_val;
                } else if (op == "*") {
                    result = current_val * const_val;
                } else {
                    return absl::InternalError(
                        fmt::format("unsupported operator: {}", op));
                }
                auto result_datum = small::type::Datum();
                result_datum.set_int64_value(result);
                new_encoded_value = small::type::encode(result_datum);
            } else {
                return absl::InternalError(
                    fmt::format("unsupported type for arithmetic: {}",
                                small::type::to_string(col_type)));
            }
        } else if (val_node->node_case == PG_QUERY__NODE__NODE_A_CONST) {
            auto datum =
                small::semantics::extract_const(val_node->a_const).value();
            new_encoded_value = small::type::encode(datum);
        } else {
            return absl::InternalError("unsupported SET value expression");
        }

        updated[column_name] = new_encoded_value;
    }

    std::vector<std::string> values;
    for (const auto& col : table->columns()) {
        values.push_back(updated.at(col.name()));
    }
    return values;
}

absl::StatusOr<UpdateResult> update(PgQuery__UpdateStmt* update_stmt,
                                    bool dispatch, int64_t commit_ts,
                                    int64_t txn_id,
                                    const std::string& coordinator_addr) {
    auto table_name = small::schema::resolve_table_name(update_stmt->relation);
    auto table_optional =
        small::catalog::CatalogManager::GetInstance()->GetTable(table_name);
    if (!table_optional) {
        return absl::InternalError(
            fmt::format("table {} not found", table_name));
    }
    const auto& table = table_optional.value();

    auto pk_or = extract_pk(update_stmt, table);
    if (!pk_or.ok()) return pk_or.status();
    const std::string pk = pk_or.value();
    UpdateResult out;
    out.final_commit_ts = commit_ts;
    out.intent_key = absl::StrFormat("/%s/%s/INTENT", table_name, pk);

    if (dispatch) {
        // Coordinator side: fan out to every peer with the txn fields.
        // Only the row's owner does anything visible; non-owners return
        // their input commit_ts unchanged. We collect the max across
        // all responses so a push reported by the owner propagates back.
        auto servers = small::gossip::get_nodes(std::nullopt);
        for (auto& [id, server] : servers) {
            small::execution::RawNode request;

            size_t len = pg_query__update_stmt__get_packed_size(update_stmt);
            auto buf = static_cast<uint8_t*>(malloc(len));
            pg_query__update_stmt__pack(update_stmt, buf);

            request.set_packed_node(buf, len);
            request.set_ts(commit_ts);
            request.set_txn_id(txn_id);
            request.set_coordinator_addr(coordinator_addr);
            free(buf);

            auto channel = grpc::CreateChannel(
                server.grpc_addr, grpc::InsecureChannelCredentials());
            auto stub = small::execution::Update::NewStub(channel);
            grpc::ClientContext context;
            small::execution::WriteResponse result;
            grpc::Status status = stub->Update(&context, request, &result);
            if (!status.ok()) {
                return absl::InternalError(
                    fmt::format("failed to update into server {}: {}",
                                server.grpc_addr, status.error_message()));
            }
            if (result.final_commit_ts() > out.final_commit_ts) {
                out.final_commit_ts = result.final_commit_ts();
            }
        }
        return out;
    }

    // Peer side (dispatch=false). Acquire the row lock, read the
    // intent-aware latest committed version_ts, push if needed, and
    // write the intent.
    auto db = small::rocks::RocksDBWrapper::GetInstance().value();

    auto row_lock =
        small::lock::LockManager::GetInstance()->Acquire(table_name, pk);

    // No row on this node => not the partition owner (or the row
    // doesn't exist anywhere). Return commit_ts unchanged.
    //
    // Use the intent-aware variant so a prior committed-but-unpromoted
    // intent on this row contributes its value as the pre-image for
    // the SET clause computation.
    auto current = small::txn::read_latest_with_intents(table_name, pk);
    if (!current.has_value()) {
        return out;
    }

    auto latest_or = small::txn::latest_committed_version_ts(table_name, pk);
    if (!latest_or.ok()) return latest_or.status();
    int64_t latest = latest_or.value();
    if (latest >= out.final_commit_ts) {
        out.final_commit_ts = latest + 1;
        SPDLOG_INFO("update push: txn_id={} {}/{} commit_ts {}->{}", txn_id,
                    table_name, pk, commit_ts, out.final_commit_ts);
    }

    auto values_or = apply_set_clause(update_stmt, table, current.value());
    if (!values_or.ok()) return values_or.status();
    db->WriteIntent(table, pk, values_or.value(), txn_id, coordinator_addr);

    return out;
}

grpc::Status UpdateServiceImpl::Update(
    grpc::ServerContext* context, const small::execution::RawNode* request,
    small::execution::WriteResponse* response) {
    SPDLOG_INFO("update request: {}", request->DebugString());

    PgQuery__UpdateStmt* node = pg_query__update_stmt__unpack(
        nullptr, request->packed_node().size(),
        reinterpret_cast<const uint8_t*>(request->packed_node().data()));

    auto result =
        update(node, /*dispatch=*/false, request->ts(), request->txn_id(),
               request->coordinator_addr());
    pg_query__update_stmt__free_unpacked(node, nullptr);

    if (!result.ok()) {
        return {grpc::StatusCode::INTERNAL,
                std::string(result.status().message())};
    }
    response->set_final_commit_ts(result->final_commit_ts);
    return grpc::Status::OK;
}

}  // namespace small::execution
