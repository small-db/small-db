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
#include <optional>
#include <string>

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
#include "src/rocks/rocks.h"
#include "src/semantics/extract.h"
#include "src/type/type.h"

// =====================================================================
// self header
// =====================================================================

#include "src/execution/update.h"

namespace small::execution {

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> update(
    PgQuery__UpdateStmt* update_stmt, bool dispatch) {
    auto table_name = update_stmt->relation->relname;
    auto table_optional =
        small::catalog::CatalogManager::GetInstance()->GetTable(table_name);
    if (!table_optional) {
        return absl::InternalError(
            fmt::format("table {} not found", table_name));
    }
    const auto& table = table_optional.value();

    if (dispatch) {
        auto servers = small::gossip::get_nodes(std::nullopt);
        for (auto& [id, server] : servers) {
            small::execution::RawNode request;

            size_t len = pg_query__update_stmt__get_packed_size(update_stmt);
            auto buf = static_cast<uint8_t*>(malloc(len));
            pg_query__update_stmt__pack(update_stmt, buf);

            request.set_packed_node(buf, len);
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
        }

        auto schema = arrow::schema({});
        return arrow::RecordBatch::Make(schema, 0, arrow::ArrayVector{});
    }

    // Local execution (dispatch=false)
    auto db = small::rocks::RocksDBWrapper::GetInstance().value();
    auto rows = db->ReadTable(table_name);

    // filter (based on where clause)
    std::map<std::string, std::map<std::string, std::string>> filtered_rows;
    std::string filter_column =
        update_stmt->where_clause->a_expr->lexpr->column_ref->fields[0]
            ->string->sval;
    auto filter_value = small::semantics::extract_const(
                            update_stmt->where_clause->a_expr->rexpr->a_const)
                            .value();
    auto encoded_filter_value = small::type::encode(filter_value);
    for (const auto& [pk, columns] : rows) {
        if (columns.count(filter_column) &&
            columns.at(filter_column) == encoded_filter_value) {
            filtered_rows[pk] = columns;
        }
    }

    // apply SET clause to filtered rows
    for (const auto& [pk, columns] : filtered_rows) {
        for (size_t i = 0; i < update_stmt->n_target_list; i++) {
            auto res_target = update_stmt->target_list[i]->res_target;
            auto column_name = std::string(res_target->name);
            auto val_node = res_target->val;

            std::string new_encoded_value;

            if (val_node->node_case == PG_QUERY__NODE__NODE_A_EXPR) {
                auto expr = val_node->a_expr;
                auto op = std::string(expr->name[0]->string->sval);

                // get current value of the referenced column
                auto ref_column = std::string(
                    expr->lexpr->column_ref->fields[0]->string->sval);
                auto current_encoded = columns.at(ref_column);

                // find column type
                small::type::Type col_type = small::type::Type::STRING;
                for (const auto& col : table->columns()) {
                    if (col.name() == column_name) {
                        col_type = col.type();
                        break;
                    }
                }

                auto current_datum =
                    small::type::decode(current_encoded, col_type);
                auto const_datum = small::semantics::extract_const(
                                       expr->rexpr->a_const)
                                       .value();

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
                    return absl::InternalError(fmt::format(
                        "unsupported type for arithmetic: {}",
                        small::type::to_string(col_type)));
                }
            } else if (val_node->node_case == PG_QUERY__NODE__NODE_A_CONST) {
                auto datum =
                    small::semantics::extract_const(val_node->a_const).value();
                new_encoded_value = small::type::encode(datum);
            } else {
                return absl::InternalError(
                    "unsupported SET value expression");
            }

            db->WriteCell(table, pk, column_name, new_encoded_value);
        }
    }

    auto schema = arrow::schema({});
    return arrow::RecordBatch::Make(schema, 0, arrow::ArrayVector{});
}

grpc::Status UpdateServiceImpl::Update(
    grpc::ServerContext* context, const small::execution::RawNode* request,
    small::execution::WriteResponse* response) {
    SPDLOG_INFO("update request: {}", request->DebugString());

    PgQuery__UpdateStmt* node = pg_query__update_stmt__unpack(
        nullptr, request->packed_node().size(),
        reinterpret_cast<const uint8_t*>(request->packed_node().data()));

    auto result = update(node, false);
    pg_query__update_stmt__free_unpacked(node, nullptr);

    if (!result.ok()) {
        return {grpc::StatusCode::INTERNAL,
                std::string(result.status().message())};
    }

    return grpc::Status::OK;
}

}  // namespace small::execution
