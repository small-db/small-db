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
#include "src/schema/const.h"
#include "src/semantics/check.h"
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

static int64_t now_millis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// COMMIT: pick one commit_ts and dispatch every buffered UPDATE under that
// timestamp. All resulting row-version keys share the same ts, so a snapshot
// reader either sees all of them or none.
//
// Note: this is "eventual atomic visibility per node" — within one node, all
// writes for the txn appear at the same ts; across nodes they share the ts
// but not the wall-clock instant of application. A reader pinned at
// snapshot_ts < commit_ts sees pre-state on every node; at >= commit_ts it
// sees the committed state on every node that has already applied. There is
// a small in-flight window between commit dispatch and last-leaf-applied
// where a read at >= commit_ts can be torn; closing that window is the next
// step (intents / commit-wait) and is intentionally out of scope here.
static absl::Status commit_transaction(TxnState& txn) {
    int64_t commit_ts = now_millis();
    for (auto& packed : txn.writes) {
        PgQuery__UpdateStmt* node = pg_query__update_stmt__unpack(
            nullptr, packed.size(), packed.data());
        if (node == nullptr) {
            return absl::InternalError(
                "failed to unpack buffered update at commit");
        }
        auto status = small::execution::update(node, /*dispatch=*/true,
                                               commit_ts);
        pg_query__update_stmt__free_unpacked(node, nullptr);
        if (!status.ok()) {
            return status.status();
        }
    }
    txn.active = false;
    txn.read_ts = 0;
    txn.writes.clear();
    return absl::OkStatus();
}

static void rollback_transaction(TxnState& txn) {
    txn.active = false;
    txn.read_ts = 0;
    txn.writes.clear();
}

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> handle_stmt(
    PgQuery__Node* stmt, SessionState& session) {
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
                case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_START:
                    session.txn.active = true;
                    session.txn.read_ts = now_millis();
                    session.txn.writes.clear();
                    return EmptyBatch();
                case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_COMMIT:
                    if (session.txn.active) {
                        return WrapEmptyStatus(
                            [&]() { return commit_transaction(session.txn); });
                    }
                    return EmptyBatch();
                case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK:
                    rollback_transaction(session.txn);
                    return EmptyBatch();
                default:
                    SPDLOG_INFO("unhandled transaction kind: {}",
                                static_cast<int>(kind));
                    return EmptyBatch();
            }
        }
        case PG_QUERY__NODE__NODE_ALTER_TABLE_STMT: {
            return WrapEmptyStatus([&]() {
                return handle_add_constraint(stmt->alter_table_stmt);
            });
            break;
        }
        case PG_QUERY__NODE__NODE_SELECT_STMT: {
            // Inside a txn, read at the snapshot pinned at BEGIN so writes
            // committed mid-txn (by us or anyone else) don't appear.
            // Outside a txn, read_ts=0 lets the dispatcher pin "now()" itself
            // so all leaves agree on one snapshot for this single read.
            int64_t read_ts =
                session.txn.active ? session.txn.read_ts : 0;
            return small::execution::query(stmt->select_stmt, true, read_ts);
            break;
        }
        case PG_QUERY__NODE__NODE_UPDATE_STMT: {
            // Inside a txn: buffer the packed UpdateStmt; the writes will
            // be replayed at COMMIT under one shared commit_ts.
            if (session.txn.active) {
                size_t len = pg_query__update_stmt__get_packed_size(
                    stmt->update_stmt);
                std::vector<uint8_t> buf(len);
                pg_query__update_stmt__pack(stmt->update_stmt, buf.data());
                session.txn.writes.push_back(std::move(buf));
                return EmptyBatch();
            }
            return small::execution::update(stmt->update_stmt, true);
            break;
        }
        case PG_QUERY__NODE__NODE_INSERT_STMT: {
            return WrapEmptyStatus(
                [&]() { return small::execution::insert(stmt->insert_stmt); });
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
