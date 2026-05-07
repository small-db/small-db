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

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "magic_enum/magic_enum.hpp"
#include "pg_query.h"
#include "pg_query.pb-c.h"
#include "spdlog/spdlog.h"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/catalog/catalog.h"
#include "src/execution/insert.h"
#include "src/execution/query.h"
#include "src/execution/update.h"
#include "src/id/generator.h"
#include "src/rocks/rocks.h"
#include "src/schema/const.h"
#include "src/schema/schema.h"
#include "src/semantics/check.h"
#include "src/semantics/extract.h"
#include "src/server_info/info.h"
#include "src/type/type.h"
#include "src/util/time/time.h"

// =====================================================================
// self header
// =====================================================================

#include "src/txn/handle.h"

namespace small::txn {

// Wall-clock ms since epoch. Used both as snapshot timestamps for
// reads and as the floor for write commit timestamps.
static int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// ===== DDL helpers (no transaction context needed) =====

static absl::Status handle_create_table(PgQuery__CreateStmt* create_stmt) {
    std::string table_name =
        small::schema::resolve_table_name(create_stmt->relation);
    std::vector<small::schema::Column> columns;

    for (int i = 0; i < create_stmt->n_table_elts; i++) {
        auto node_case = create_stmt->table_elts[i]->node_case;
        switch (node_case) {
            case PG_QUERY__NODE__NODE_COLUMN_DEF: {
                auto column_def = create_stmt->table_elts[i]->column_def;

                int name_id =
                    static_cast<int>(column_def->type_name->n_names) - 1;
                auto type_name = ::semantics::is_string(
                    column_def->type_name->names[name_id]);

                bool primary_key = false;
                for (int j = 0; j < column_def->n_constraints; j++) {
                    auto constraint = column_def->constraints[j]->constraint;
                    switch (constraint->contype) {
                        case PG_QUERY__CONSTR_TYPE__CONSTR_PRIMARY:
                            primary_key = true;
                            SPDLOG_INFO("constraint->contype: {}",
                                        static_cast<int>(constraint->contype));
                            break;
                        default:
                            break;
                    }
                }

                auto type = small::type::from_ast_string(type_name.value());
                if (!type.ok()) {
                    SPDLOG_ERROR("unknown type: {}", type_name.value());
                    return type.status();
                }
                auto column = small::schema::Column();
                column.set_name(column_def->colname);
                column.set_type(type.value());
                if (primary_key) {
                    column.set_is_primary_key(true);
                }
                columns.push_back(column);
                break;
            }
            case PG_QUERY__NODE__NODE_CONSTRAINT: {
                SPDLOG_ERROR("constraint");
                break;
            }
            default:
                SPDLOG_ERROR("unknown table element, node_case: {}",
                             static_cast<int>(node_case));
                break;
        }
    }

    auto status = small::catalog::CatalogManager::GetInstance()->CreateTable(
        table_name, columns);
    if (!status.ok()) {
        SPDLOG_ERROR("create table failed: {}", status.ToString());
        return status;
    }

    if (create_stmt->partspec != nullptr) {
        auto strategy = create_stmt->partspec->strategy;
        if (create_stmt->partspec->n_part_params != 1) {
            SPDLOG_ERROR("number of part params: {}",
                         create_stmt->partspec->n_part_params);
            return absl::OkStatus();
        }

        auto partition_column = std::string(
            create_stmt->partspec->part_params[0]->partition_elem->name);

        auto status =
            small::catalog::CatalogManager::GetInstance()->SetPartition(
                table_name, partition_column, strategy);
        if (!status.ok()) {
            SPDLOG_ERROR("set partitioning failed: {}", status.ToString());
            return status;
        }
    }

    return absl::OkStatus();
}

static absl::Status handle_drop_table(PgQuery__DropStmt* drop_stmt) {
    auto relname = drop_stmt->objects[0]->list->items[0]->string->sval;
    auto table_name =
        std::string(small::schema::DEFAULT_SCHEMA) + "." + std::string(relname);
    return small::catalog::CatalogManager::GetInstance()->DropTable(table_name);
}

static absl::Status handle_add_partition(PgQuery__CreateStmt* create_stmt) {
    auto table_name = small::schema::resolve_table_name(
        create_stmt->inh_relations[0]->range_var);
    auto partition_name = create_stmt->relation->relname;

    std::vector<std::string> values;
    for (int i = 0; i < create_stmt->partbound->n_listdatums; i++) {
        auto v = small::semantics::a_const_to_string(
            create_stmt->partbound->listdatums[i]->a_const);
        if (!v.ok()) return v.status();
        values.push_back(std::move(v.value()));
    }

    return small::catalog::CatalogManager::GetInstance()
        ->ListPartitionAddValues(table_name, partition_name, values);
}

static absl::Status handle_add_constraint(PgQuery__AlterTableStmt* alter_stmt) {
    auto partition_name = alter_stmt->relation->relname;
    auto expr =
        alter_stmt->cmds[0]->alter_table_cmd->def->constraint->raw_expr->a_expr;
    auto lexpr = expr->lexpr->column_ref->fields[0]->string->sval;
    auto op = expr->name[0]->string->sval;
    auto rexpr = expr->rexpr->a_const->sval->sval;
    SPDLOG_INFO("partition_name: {}, lexpr: {}, op: {}, rexpr: {}",
                partition_name, lexpr, op, rexpr);
    return small::catalog::CatalogManager::GetInstance()
        ->ListPartitionAddConstraint(partition_name,
                                     std::make_pair(lexpr, rexpr));
}

static std::shared_ptr<arrow::RecordBatch> EmptyBatch() {
    auto schema = arrow::schema({});
    arrow::ArrayVector outputs;
    return arrow::RecordBatch::Make(schema, 0, outputs);
}

static absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> WrapEmptyStatus(
    const std::function<absl::Status()>& func) {
    absl::Status status = func();
    if (status.ok()) return EmptyBatch();
    return status;
}

// ===== Lifecycle =====

absl::Status Txn::Begin() {
    if (active_) {
        return absl::FailedPreconditionError("nested BEGIN is not supported");
    }
    active_ = true;
    txn_id_ = id::generate_id();
    start_ts_ = now_ms();
    write_ts_ = start_ts_;

    auto db = small::rocks::RocksDBWrapper::GetInstance();
    if (!db.ok()) return db.status();
    db.value()->WriteTxnRecord(
        txn_id_,
        small::rocks::TxnRecord{small::rocks::TxnStatus::ACTIVE, start_ts_,
                                write_ts_, {}});
    SPDLOG_INFO("begin_txn: txn_id={} start_ts={} ({})", txn_id_, start_ts_,
                small::util::FormatTsMs(start_ts_));
    return absl::OkStatus();
}

absl::Status Txn::Commit() {
    if (!active_) {
        return absl::FailedPreconditionError("COMMIT outside of BEGIN");
    }
    // Mechanism A from closed_timestamps.md: bump write_ts to the wall
    // clock at this moment before promoting it to the final commit
    // timestamp. Any reader whose snapshot_ts < now() is reading "in
    // the past" relative to this commit, so this txn must not be
    // visible to them; the bump enforces that by ensuring the final
    // commit_ts is strictly greater than any active reader's snapshot
    // that has not yet waited past T_closed on the owners.
    int64_t now = now_ms();
    if (now > write_ts_) {
        write_ts_ = now;
    }
    // After this point, write_ts_ is the txn's final commit timestamp.
    SPDLOG_INFO("commit_txn: txn_id={} start_ts={} ({}) commit_ts={} ({})",
                txn_id_, start_ts_, small::util::FormatTsMs(start_ts_),
                write_ts_, small::util::FormatTsMs(write_ts_));
    auto db = small::rocks::RocksDBWrapper::GetInstance();
    if (!db.ok()) return db.status();
    db.value()->SetTxnStatus(txn_id_, small::rocks::TxnStatus::COMMITTED,
                             write_ts_);
    // Leave txn_id_/start_ts_/write_ts_ populated after the txn ends
    // so callers (notably tests) can inspect the final commit_ts that
    // landed on disk. `active_ = false` is the source of truth for
    // "this Txn is no longer driving statements"; Begin() resets the
    // other fields when starting the next transaction.
    active_ = false;
    return absl::OkStatus();
}

absl::Status Txn::Rollback() {
    if (!active_) {
        return absl::FailedPreconditionError("ROLLBACK outside of BEGIN");
    }
    SPDLOG_INFO("rollback_txn: txn_id={}", txn_id_);
    auto db = small::rocks::RocksDBWrapper::GetInstance();
    if (!db.ok()) return db.status();
    db.value()->SetTxnStatus(txn_id_, small::rocks::TxnStatus::ABORTED, 0);
    active_ = false;
    return absl::OkStatus();
}

// Run `body` inside a transaction. If none is active, begin an
// implicit one before `body` and commit after; otherwise just run
// `body` and let the explicit BEGIN/COMMIT control the boundary.
template <typename F>
static absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> run_with_txn(
    Txn& txn, F&& body) {
    bool implicit = !txn.active();
    if (implicit) {
        auto status = txn.Begin();
        if (!status.ok()) return status;
    }
    auto result = body();
    if (implicit) {
        auto commit_status =
            result.ok() ? txn.Commit() : txn.Rollback();
        if (!commit_status.ok()) return commit_status;
    }
    return result;
}

// ===== Dispatch =====

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> Txn::ExecuteNode(
    PgQuery__Node* stmt) {
    switch (stmt->node_case) {
        case PG_QUERY__NODE__NODE_CREATE_STMT: {
            auto create_stmt = stmt->create_stmt;
            if (create_stmt->n_inh_relations == 0) {
                return WrapEmptyStatus(
                    [&]() { return handle_create_table(create_stmt); });
            }
            return WrapEmptyStatus(
                [&]() { return handle_add_partition(create_stmt); });
        }
        case PG_QUERY__NODE__NODE_DROP_STMT: {
            return WrapEmptyStatus(
                [&]() { return handle_drop_table(stmt->drop_stmt); });
        }
        case PG_QUERY__NODE__NODE_TRANSACTION_STMT: {
            auto kind = stmt->transaction_stmt->kind;
            switch (kind) {
                case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_BEGIN:
                case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_START:
                    return WrapEmptyStatus([&]() { return Begin(); });
                case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_COMMIT:
                    return WrapEmptyStatus([&]() { return Commit(); });
                case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK:
                    return WrapEmptyStatus([&]() { return Rollback(); });
                default:
                    SPDLOG_INFO("ignoring transaction stmt kind: {}",
                                magic_enum::enum_name(kind));
                    return EmptyBatch();
            }
        }
        case PG_QUERY__NODE__NODE_ALTER_TABLE_STMT: {
            return WrapEmptyStatus([&]() {
                return handle_add_constraint(stmt->alter_table_stmt);
            });
        }
        case PG_QUERY__NODE__NODE_SELECT_STMT: {
            return run_with_txn(*this, [&]() {
                return small::execution::query(stmt->select_stmt, true,
                                               start_ts_);
            });
        }
        case PG_QUERY__NODE__NODE_UPDATE_STMT: {
            return run_with_txn(
                *this,
                [&]() -> absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> {
                    auto info = small::server_info::get_info();
                    if (!info.ok()) return info.status();
                    auto db = small::rocks::RocksDBWrapper::GetInstance();
                    if (!db.ok()) return db.status();
                    auto result = small::execution::update(
                        stmt->update_stmt, true, write_ts_,
                        txn_id_, info.value()->grpc_addr);
                    if (!result.ok()) return result.status();
                    if (result->final_write_ts > write_ts_) {
                        write_ts_ = result->final_write_ts;
                        db.value()->UpdateTxnWriteTs(txn_id_, write_ts_);
                    }
                    db.value()->AppendTxnIntentKey(txn_id_, result->intent_key);
                    return EmptyBatch();
                });
        }
        case PG_QUERY__NODE__NODE_INSERT_STMT: {
            return run_with_txn(
                *this,
                [&]() -> absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> {
                    auto status = small::execution::insert(stmt->insert_stmt,
                                                           start_ts_);
                    if (!status.ok()) return status;
                    return EmptyBatch();
                });
        }
        default:
            SPDLOG_ERROR("unknown statement, node_case: {}",
                         magic_enum::enum_name(stmt->node_case));
            return absl::InternalError(
                fmt::format("unknown statement, node_case: {}",
                            magic_enum::enum_name(stmt->node_case)));
    }
}

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> Txn::Execute(
    std::string_view sql) {
    std::string sql_str(sql);
    PgQueryProtobufParseResult parsed = pg_query_parse_protobuf_opts(
        sql_str.c_str(), PG_QUERY_PARSE_DEFAULT);
    if (parsed.error != nullptr) {
        std::string msg = parsed.error->message;
        pg_query_free_protobuf_parse_result(parsed);
        return absl::InvalidArgumentError(
            absl::StrFormat("parse error: %s", msg));
    }

    auto unpacked = pg_query__parse_result__unpack(
        nullptr, parsed.parse_tree.len,
        reinterpret_cast<const uint8_t*>(parsed.parse_tree.data));
    pg_query_free_protobuf_parse_result(parsed);

    if (unpacked == nullptr || unpacked->n_stmts == 0) {
        if (unpacked != nullptr) {
            pg_query__parse_result__free_unpacked(unpacked, nullptr);
        }
        return absl::InvalidArgumentError("empty statement");
    }

    auto result = ExecuteNode(unpacked->stmts[0]->stmt);
    pg_query__parse_result__free_unpacked(unpacked, nullptr);
    return result;
}

absl::StatusOr<std::string> Txn::QueryScalar(std::string_view sql) {
    auto batch_or = Execute(sql);
    if (!batch_or.ok()) return batch_or.status();
    auto batch = batch_or.value();
    if (batch->num_rows() == 0) {
        return absl::NotFoundError("query returned no rows");
    }
    if (batch->num_columns() == 0) {
        return absl::InternalError("query returned no columns");
    }
    auto scalar_or = batch->column(0)->GetScalar(0);
    if (!scalar_or.ok()) {
        return absl::InternalError(scalar_or.status().ToString());
    }
    return scalar_or.ValueOrDie()->ToString();
}

}  // namespace small::txn
