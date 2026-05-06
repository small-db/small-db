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
#include <memory>
#include <string>
#include <utility>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

// magic_enum
#include "magic_enum/magic_enum.hpp"

// pg_query
#include "pg_query.pb-c.h"

// spdlog
#include "spdlog/spdlog.h"

// absl
#include "absl/status/status.h"

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
#include "src/semantics/check.h"
#include "src/server_info/info.h"
#include "src/type/type.h"

// =====================================================================
// self header
// =====================================================================

#include "src/server/stmt_handler.h"

namespace small::stmt_handler {

absl::Status handle_create_table(PgQuery__CreateStmt* create_stmt) {
    std::string table_name =
        small::schema::resolve_table_name(create_stmt->relation);
    std::vector<small::schema::Column> columns;

    for (int i = 0; i < create_stmt->n_table_elts; i++) {
        auto node_case = create_stmt->table_elts[i]->node_case;
        switch (node_case) {
            case PG_QUERY__NODE__NODE_COLUMN_DEF: {
                auto column_def = create_stmt->table_elts[i]->column_def;

                // choose the last name as the type name
                // Q: why?
                // A:
                // int -> [pg_catalog, int4]
                // double -> [pg_catalog, float8]
                // string -> [string]
                int name_id = column_def->type_name->n_names - 1;

                auto type_name =
                    semantics::is_string(column_def->type_name->names[name_id]);

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

    if (create_stmt->partspec != NULL) {
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

absl::Status handle_drop_table(PgQuery__DropStmt* drop_stmt) {
    auto relname = drop_stmt->objects[0]->list->items[0]->string->sval;
    auto table_name =
        std::string(small::schema::DEFAULT_SCHEMA) + "." + std::string(relname);
    return small::catalog::CatalogManager::GetInstance()->DropTable(table_name);
}

absl::Status handle_add_partition(PgQuery__CreateStmt* create_stmt) {
    auto table_name = small::schema::resolve_table_name(
        create_stmt->inh_relations[0]->range_var);
    auto partition_name = create_stmt->relation->relname;

    std::vector<std::string> values;
    for (int i = 0; i < create_stmt->partbound->n_listdatums; i++) {
        const auto& datum = create_stmt->partbound->listdatums[i];
        values.push_back(datum->a_const->sval->sval);
    }

    return small::catalog::CatalogManager::GetInstance()
        ->ListPartitionAddValues(table_name, partition_name, values);
}

absl::Status handle_add_constraint(PgQuery__AlterTableStmt* alter_stmt) {
    auto subtype = alter_stmt->cmds[0]->alter_table_cmd->subtype;

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

std::shared_ptr<arrow::RecordBatch> EmptyBatch() {
    auto schema = arrow::schema({});
    arrow::ArrayVector outputs;
    auto empty_batch = arrow::RecordBatch::Make(schema, 0, outputs);
    return empty_batch;
}

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> WrapEmptyStatus(
    const std::function<absl::Status()>& func) {
    absl::Status status = func();

    if (status.ok()) {
        return EmptyBatch();
    } else {
        return status;
    }
}

// Wall-clock ms since epoch. Used both as snapshot timestamps for
// reads and as the floor for write commit timestamps.
static int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// Begin a new transaction on this connection. Generates a fresh txn_id,
// stamps start_ts, persists /_txn/<txn_id> with status = ACTIVE on the
// coordinator's RocksDB so readers resolving this txn's intents (and a
// future recovery path) have an on-disk anchor.
static absl::Status begin_txn(TxnState& txn) {
    if (txn.active) {
        return absl::FailedPreconditionError("nested BEGIN is not supported");
    }
    txn.active = true;
    txn.txn_id = id::generate_id();
    txn.start_ts = now_ms();
    txn.commit_ts = txn.start_ts;

    auto db = small::rocks::RocksDBWrapper::GetInstance();
    if (!db.ok()) return db.status();
    db.value()->WriteTxnRecord(
        txn.txn_id,
        small::rocks::TxnRecord{small::rocks::TxnStatus::ACTIVE, txn.start_ts,
                                txn.commit_ts, {}});
    SPDLOG_INFO("begin_txn: txn_id={} start_ts={}", txn.txn_id, txn.start_ts);
    return absl::OkStatus();
}

// Flip /_txn/<txn_id> to COMMITTED. The single Put is the atomicity
// boundary -- every reader that subsequently resolves any of this txn's
// intents observes the new status. Resets `txn` to inactive.
static absl::Status commit_txn(TxnState& txn) {
    if (!txn.active) {
        return absl::FailedPreconditionError("COMMIT outside of BEGIN");
    }
    SPDLOG_INFO("commit_txn: txn_id={} start_ts={} commit_ts={}", txn.txn_id,
                txn.start_ts, txn.commit_ts);
    auto db = small::rocks::RocksDBWrapper::GetInstance();
    if (!db.ok()) return db.status();
    db.value()->SetTxnStatus(txn.txn_id, small::rocks::TxnStatus::COMMITTED,
                             txn.commit_ts);
    txn.active = false;
    txn.txn_id = 0;
    txn.start_ts = 0;
    txn.commit_ts = 0;
    return absl::OkStatus();
}

// Flip /_txn/<txn_id> to ABORTED. Intents on disk stay (no active
// cleanup); readers resolving them will skip per the chapter's design.
// Resets `txn` to inactive.
static absl::Status rollback_txn(TxnState& txn) {
    if (!txn.active) {
        return absl::FailedPreconditionError("ROLLBACK outside of BEGIN");
    }
    SPDLOG_INFO("rollback_txn: txn_id={}", txn.txn_id);
    auto db = small::rocks::RocksDBWrapper::GetInstance();
    if (!db.ok()) return db.status();
    db.value()->SetTxnStatus(txn.txn_id, small::rocks::TxnStatus::ABORTED, 0);
    txn.active = false;
    txn.txn_id = 0;
    txn.start_ts = 0;
    txn.commit_ts = 0;
    return absl::OkStatus();
}

// Run `body` inside a transaction. If none is active on entry, begin
// an implicit one before `body` and commit after; otherwise just run
// `body` and let the explicit BEGIN/COMMIT control the boundary. The
// chapter's "every statement runs inside a transaction" rule.
template <typename F>
static absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> run_with_txn(
    TxnState& txn, F&& body) {
    bool implicit = !txn.active;
    if (implicit) {
        auto status = begin_txn(txn);
        if (!status.ok()) return status;
    }
    auto result = body();
    if (implicit) {
        auto commit_status =
            result.ok() ? commit_txn(txn) : rollback_txn(txn);
        if (!commit_status.ok()) return commit_status;
    }
    return result;
}

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> handle_stmt(
    PgQuery__Node* stmt, TxnState& txn) {
    switch (stmt->node_case) {
        case PG_QUERY__NODE__NODE_CREATE_STMT: {
            auto create_stmt = stmt->create_stmt;
            if (create_stmt->n_inh_relations == 0) {
                return WrapEmptyStatus(
                    [&]() { return handle_create_table(create_stmt); });
            } else {
                return WrapEmptyStatus(
                    [&]() { return handle_add_partition(create_stmt); });
            }
            break;
        }
        case PG_QUERY__NODE__NODE_DROP_STMT: {
            return WrapEmptyStatus(
                [&]() { return handle_drop_table(stmt->drop_stmt); });
            break;
        }
        case PG_QUERY__NODE__NODE_TRANSACTION_STMT: {
            auto kind = stmt->transaction_stmt->kind;
            switch (kind) {
                case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_BEGIN:
                case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_START: {
                    return WrapEmptyStatus([&]() { return begin_txn(txn); });
                }
                case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_COMMIT: {
                    return WrapEmptyStatus([&]() { return commit_txn(txn); });
                }
                case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK: {
                    return WrapEmptyStatus(
                        [&]() { return rollback_txn(txn); });
                }
                default:
                    SPDLOG_INFO("ignoring transaction stmt kind: {}",
                                magic_enum::enum_name(kind));
                    return EmptyBatch();
            }
            break;
        }
        case PG_QUERY__NODE__NODE_ALTER_TABLE_STMT: {
            return WrapEmptyStatus([&]() {
                return handle_add_constraint(stmt->alter_table_stmt);
            });
            break;
        }
        case PG_QUERY__NODE__NODE_SELECT_STMT: {
            return run_with_txn(txn, [&]() {
                return small::execution::query(stmt->select_stmt, true,
                                               txn.start_ts);
            });
            break;
        }
        case PG_QUERY__NODE__NODE_UPDATE_STMT: {
            return run_with_txn(
                txn,
                [&]() -> absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> {
                    auto info = small::server_info::get_info();
                    if (!info.ok()) return info.status();
                    auto db = small::rocks::RocksDBWrapper::GetInstance();
                    if (!db.ok()) return db.status();
                    auto result = small::execution::update(
                        stmt->update_stmt, /*dispatch=*/true, txn.commit_ts,
                        txn.txn_id, info.value()->grpc_addr);
                    if (!result.ok()) return result.status();
                    if (result->final_commit_ts > txn.commit_ts) {
                        txn.commit_ts = result->final_commit_ts;
                        db.value()->UpdateTxnCommitTs(txn.txn_id,
                                                      txn.commit_ts);
                    }
                    db.value()->AppendTxnIntentKey(txn.txn_id,
                                                   result->intent_key);
                    return EmptyBatch();
                });
            break;
        }
        case PG_QUERY__NODE__NODE_INSERT_STMT: {
            return run_with_txn(
                txn,
                [&]() -> absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> {
                    auto status = small::execution::insert(stmt->insert_stmt,
                                                           txn.start_ts);
                    if (!status.ok()) return status;
                    return EmptyBatch();
                });
            break;
        }
        default:
            SPDLOG_ERROR("unknown statement, node_case: {}",
                         magic_enum::enum_name(stmt->node_case));
            return absl::InternalError(
                fmt::format("unknown statement, node_case: {}",
                            magic_enum::enum_name(stmt->node_case)));
            break;
    }

    return EmptyBatch();
}

}  // namespace small::stmt_handler
