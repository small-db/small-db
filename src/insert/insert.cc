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

#include <memory>
#include <string>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

// absl
#include "absl/status/status.h"

// pg_query
#include "pg_query.pb-c.h"

// spdlog
#include "spdlog/spdlog.h"

// grpc
#include "grpcpp/create_channel.h"

// =====================================================================
// local libraries
// =====================================================================

#include "src/catalog/catalog.h"
#include "src/encode/encode.h"
#include "src/peers/server_registry.h"
#include "src/semantics/extract.h"

// =====================================================================
// protobuf generated files
// =====================================================================

#include "insert.grpc.pb.h"
#include "insert.pb.h"

// =====================================================================
// self header
// =====================================================================

#include "src/insert/insert.h"

namespace small::insert {

absl::Status insert(PgQuery__InsertStmt* insert_stmt) {
    auto table_name = insert_stmt->relation->relname;
    auto result = small::catalog::Catalog::GetInstance()->GetTable(table_name);
    if (!result) {
        return absl::InternalError(
            fmt::format("table {} not found", table_name));
    }

    const auto& table = result.value();
    if (auto* listP =
            std::get_if<small::schema::ListPartition>(&table->partition)) {
        auto partition_column = listP->column_name;

        // get partition column id (in the insert statement)
        int partition_column_id = -1;
        for (int i = 0; i < insert_stmt->n_cols; i++) {
            if (insert_stmt->cols[i]->res_target->name == partition_column) {
                partition_column_id = i;
                break;
            }
        }

        if (partition_column_id == -1) {
            return absl::InternalError(
                fmt::format("partition column {} not found", partition_column));
        }

        // process row by row
        int row_count = insert_stmt->select_stmt->select_stmt->n_values_lists;
        for (int row_id = 0; row_id < row_count; row_id++) {
            // get the partition value
            auto row =
                insert_stmt->select_stmt->select_stmt->values_lists[row_id];
            auto partition_value =
                row->list->items[partition_column_id]->a_const->sval->sval;
            SPDLOG_INFO("partition value: {}", partition_value);

            // get the partition
            auto partition = listP->lookup(partition_value);
            if (!partition) {
                return absl::InternalError(fmt::format(
                    "partition not found for value {}", partition_value));
            }

            for (const auto& [key, value] : partition->constraints) {
                SPDLOG_INFO("partition constraint: {} = {}", key, value);
            }

            // search a server for the partition
            auto servers =
                small::server_registry::get_servers(partition->constraints);
            if (servers.empty()) {
                return absl::InternalError(fmt::format(
                    "no server found for partition {}", partition_value));
            }
            if (servers.size() > 1) {
                return absl::InternalError(
                    fmt::format("multiple servers found for partition {}",
                                partition_value));
            }

            // insert the row into the server
            auto server = servers[0];

            small::insert::Row request;
            for (int i = 0; i < insert_stmt->n_cols; i++) {
                auto column_name = insert_stmt->cols[i]->res_target->name;
                auto datum = small::semantics::extract_const(
                    row->list->items[i]->a_const);
                if (!datum.has_value()) {
                    return absl::InternalError(fmt::format(
                        "failed to extract const for column {}", column_name));
                }
                auto column_value = small::encode::encode(datum.value());
                request.add_column_names(column_name);
                request.add_column_values(column_value);
            }
            request.set_table_name(table_name);
            SPDLOG_INFO("insert row: {}", request.DebugString());

            auto channel = grpc::CreateChannel(
                server.grpc_addr, grpc::InsecureChannelCredentials());
            auto stub = small::insert::Insert::NewStub(channel);
            grpc::ClientContext context;
            small::insert::InsertReply result;
            grpc::Status status = stub->Insert(&context, request, &result);
            if (!status.ok()) {
                return absl::InternalError(
                    fmt::format("failed to insert row into server {}: {}",
                                server.grpc_addr, status.error_message()));
            }
        }

        return absl::OkStatus();
    } else {
        // no partition, unimplemented
        return absl::UnimplementedError(
            fmt::format("insert into table {} without partition is not "
                        "supported yet",
                        table_name));
    }
}

grpc::Status InsertService::Insert(grpc::ServerContext* context,
                                   const small::insert::Row* request,
                                   small::insert::InsertReply* response) {
    SPDLOG_INFO("insert request: {}", request->DebugString());

    // auto info = small::server_base::get_info();
    // if (!info.ok())
    //     return grpc::Status(grpc::StatusCode::INTERNAL,
    //                         "failed to get server info");
    // std::string db_path = info.value()->db_path;
    // auto db = small::rocks::RocksDBWrapper::GetInstance(db_path, {});

    // // get the table
    // auto result =
    //     small::catalog::Catalog::GetInstance()->GetTable(request->table_name());
    // if (!result) {
    //     return grpc::Status(grpc::StatusCode::NOT_FOUND,
    //                         fmt::format("table {} not found, server: {}",
    //                                     request->table_name(), db_path));
    // }
    // auto table = result.value();

    // const auto& column_values = request->column_values();
    // std::vector<std::string> values;
    // for (const auto& value : column_values) {
    //     values.push_back(value);
    // }
    // db->WriteRowWire(table, values);
    return grpc::Status::OK;
}

}  // namespace small::insert
