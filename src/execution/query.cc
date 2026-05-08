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
#include <string>
#include <utility>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

// pg_query
#include "pg_query.h"
#include "pg_query.pb-c.h"

// spdlog
#include "spdlog/spdlog.h"

// arrow
#include "arrow/api.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/status.h"
#include "arrow/table.h"

// arrow gandiva
#include "gandiva/projector.h"
#include "gandiva/tree_expr_builder.h"

// magic_enum
#include "magic_enum/magic_enum.hpp"

// grpc
#include "grpcpp/create_channel.h"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/catalog/catalog.h"
#include "src/closedts/registry.h"
#include "src/gossip/gossip.h"
#include "src/rocks/rocks.h"
#include "src/schema/const.h"
#include "src/semantics/extract.h"
#include "src/server_info/info.h"
#include "src/txn/txn.h"
#include "src/type/type.h"
#include "src/util/time/time.h"

// =====================================================================
// self header
// =====================================================================

#include "src/execution/query.h"

namespace small::execution {

std::shared_ptr<arrow::Schema> get_input_schema(
    const small::schema::Table& table) {
    arrow::FieldVector fields;
    for (const auto& column : table.columns()) {
        fields.push_back(arrow::field(
            column.name(), small::type::get_gandiva_type(column.type())));
    }
    return arrow::schema(fields);
}

// Builders in the table's column order.
std::vector<std::shared_ptr<arrow::ArrayBuilder>> get_builders(
    const std::shared_ptr<small::schema::Table>& table) {
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders;
    for (const auto& column : table->columns()) {
        switch (column.type()) {
            case small::type::Type::INT64:
                builders.push_back(std::make_shared<arrow::Int64Builder>());
                break;
            case small::type::Type::STRING:
                builders.push_back(std::make_shared<arrow::StringBuilder>());
                break;
            default:
                SPDLOG_ERROR("unsupported type: {}",
                             small::type::to_string(column.type()));
                break;
        }
    }
    return builders;
}

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> query(
    PgQuery__SelectStmt* select_stmt, bool dispatch, int64_t snapshot_ts) {
    auto table_name = small::schema::resolve_table_name(
        select_stmt->from_clause[0]->range_var);

    SPDLOG_INFO("query: table={} dispatch={} snapshot_ts={} ({})", table_name,
                dispatch, snapshot_ts, small::util::FormatTsMs(snapshot_ts));

    auto table_optional =
        small::catalog::CatalogManager::GetInstance()->GetTable(table_name);
    if (!table_optional) {
        return absl::Status(absl::StatusCode::kNotFound,
                            "table not found: " + table_name);
    }

    // Fan out for partitioned tables so the caller sees every partition, not
    // just the rows owned by this node. System / non-partitioned tables are
    // replicated identically on every node, so fall through to local execution.
    if (dispatch && table_optional.value()->partition().has_list_partition()) {
        size_t packed_len = pg_query__select_stmt__get_packed_size(select_stmt);
        std::vector<uint8_t> packed(packed_len);
        pg_query__select_stmt__pack(select_stmt, packed.data());

        auto servers = small::gossip::get_nodes(std::nullopt);
        std::vector<std::shared_ptr<arrow::RecordBatch>> remote_batches;
        remote_batches.reserve(servers.size());

        for (auto& [id, server] : servers) {
            small::execution::RawNode request;
            request.set_packed_node(packed.data(), packed_len);
            request.set_ts(snapshot_ts);

            auto channel = grpc::CreateChannel(
                server.grpc_addr, grpc::InsecureChannelCredentials());
            auto stub = small::execution::Query::NewStub(channel);
            grpc::ClientContext context;
            small::execution::QueryResponse response;
            auto status = stub->Query(&context, request, &response);
            if (!status.ok()) {
                return absl::InternalError(
                    fmt::format("failed to query server {}: {}",
                                server.grpc_addr, status.error_message()));
            }

            // Move the IPC bytes into an owning Buffer; otherwise the
            // zero-copy arrays in the deserialized RecordBatch would point
            // back into `response.ipc_bytes()` and dangle once `response`
            // is destroyed at end-of-iteration.
            auto buf = arrow::Buffer::FromString(
                std::move(*response.mutable_ipc_bytes()));
            auto reader_input = std::make_shared<arrow::io::BufferReader>(buf);
            auto reader_result =
                arrow::ipc::RecordBatchStreamReader::Open(reader_input);
            if (!reader_result.ok()) {
                return absl::InternalError(fmt::format(
                    "failed to open IPC reader from {}: {}", server.grpc_addr,
                    reader_result.status().ToString()));
            }
            auto reader = reader_result.ValueOrDie();
            std::shared_ptr<arrow::RecordBatch> batch;
            auto read_status = reader->ReadNext(&batch);
            if (!read_status.ok()) {
                return absl::InternalError(
                    fmt::format("failed to read IPC batch from {}: {}",
                                server.grpc_addr, read_status.ToString()));
            }
            if (batch) {
                remote_batches.push_back(batch);
            }
        }

        if (remote_batches.empty()) {
            return absl::InternalError(
                "fan-out returned no batches from any server");
        }

        auto output_schema = remote_batches[0]->schema();
        int64_t total_rows = 0;
        for (const auto& b : remote_batches) {
            total_rows += b->num_rows();
        }
        std::vector<std::shared_ptr<arrow::Array>> combined_columns;
        for (int c = 0; c < output_schema->num_fields(); ++c) {
            std::vector<std::shared_ptr<arrow::Array>> chunks;
            chunks.reserve(remote_batches.size());
            for (const auto& b : remote_batches) {
                chunks.push_back(b->column(c));
            }
            auto concat = arrow::Concatenate(chunks);
            if (!concat.ok()) {
                return absl::InternalError(fmt::format(
                    "column concat failed: {}", concat.status().ToString()));
            }
            combined_columns.push_back(concat.ValueOrDie());
        }
        return arrow::RecordBatch::Make(output_schema, total_rows,
                                        combined_columns);
    }

    auto input_schema = get_input_schema(*table_optional.value());
    SPDLOG_INFO("schema: {}", input_schema->ToString());

    // Closed-timestamp gate: wait until the local registry's T_closed
    // is at or above our snapshot_ts before scanning. This is what
    // closes the cross-shard partial-read race traced in
    // small-db-book/src/distributed_database/closed_timestamps.md --
    // the wait guarantees no in-flight writer with eventual
    // write_ts <= snapshot_ts will appear on this node after we
    // start scanning.
    constexpr auto kClosedTsTimeout = std::chrono::seconds(2);
    bool closed_ok =
        small::closedts::InFlightRegistry::GetInstance()->WaitUntilSafeToRead(
            snapshot_ts, kClosedTsTimeout);
    if (!closed_ok) {
        SPDLOG_WARN(
            "closed-ts gate timed out at snapshot_ts={} ({}); proceeding "
            "with potentially-incomplete view",
            snapshot_ts, small::util::FormatTsMs(snapshot_ts));
    }

    auto rows = small::txn::read_table_at_snapshot(table_name, snapshot_ts);

    if (select_stmt->where_clause != nullptr) {
        auto expr = select_stmt->where_clause->a_expr;
        auto filter_column =
            std::string(expr->lexpr->column_ref->fields[0]->string->sval);
        auto filter_value =
            small::semantics::extract_const(expr->rexpr->a_const).value();
        auto encoded_filter_value = small::type::encode(filter_value);

        std::map<std::string, std::map<std::string, std::string>> filtered;
        for (const auto& [pk, columns] : rows) {
            if (columns.count(filter_column) &&
                columns.at(filter_column) == encoded_filter_value) {
                filtered[pk] = columns;
            }
        }
        rows = filtered;
    }

    auto builders = get_builders(table_optional.value());

    for (const auto& [pk, columns] : rows) {
        SPDLOG_INFO("pk: {}, columns: {}", pk, nlohmann::json(columns).dump());

        for (const auto& column : table_optional.value()->columns()) {
            SPDLOG_INFO("column: {}", column.name());
        }

        for (int i = 0; i < table_optional.value()->columns().size(); i++) {
            const auto& column = table_optional.value()->columns()[i];
            const auto& builder = builders[i];

            if (!columns.contains(column.name())) {
                SPDLOG_INFO("json: {}", nlohmann::json(columns).dump());
                SPDLOG_ERROR("column not found in json: {}", column.name());
                return absl::Status(absl::StatusCode::kInvalidArgument,
                                    "column not found in json");
            }

            switch (column.type()) {
                case small::type::Type::INT64: {
                    auto int_builder =
                        std::dynamic_pointer_cast<arrow::Int64Builder>(builder);
                    int64_t int_value =
                        small::type::decode(columns.at(column.name()),
                                            small::type::Type::INT64)
                            .int64_value();
                    auto result = int_builder->Append(int_value);
                    if (!result.ok()) {
                        return absl::Status(
                            absl::StatusCode::kInternal,
                            fmt::format("failed to append value, error {}",
                                        result.ToString()));
                    }
                    break;
                }
                case small::type::Type::STRING: {
                    auto string_builder =
                        std::dynamic_pointer_cast<arrow::StringBuilder>(
                            builder);
                    SPDLOG_INFO("column: {}", column.name());
                    std::string string_value =
                        small::type::decode(columns.at(column.name()),
                                            small::type::Type::STRING)
                            .string_value();
                    SPDLOG_INFO("string_value: {}", string_value);

                    if (table_name == "system.tables" &&
                        column.name() == "columns") {
                        // TODO: generalize this branch.
                        // input:
                        // {"columns":[{"name":"id","type":"INT64","is_primary_key":true},{"name":"name","type":"STRING","is_primary_key":false},{"name":"balance","type":"INT64","is_primary_key":false},{"name":"country","type":"STRING","is_primary_key":false}]}
                        // output: int(PK), name:str, balance:int, country:str
                        std::vector<small::schema::Column> columns;
                        nlohmann::json::parse(string_value).get_to(columns);

                        for (const auto& col : columns) {
                            SPDLOG_INFO("col: {}", col.name());
                        }

                        string_value = "";
                        for (int i = 0; i < columns.size(); i++) {
                            const auto& col = columns[i];
                            string_value += col.name();
                            string_value += ":";
                            string_value += small::type::to_string(col.type());
                            if (col.is_primary_key()) {
                                string_value += "(PK)";
                            }
                            if (i != columns.size() - 1) {
                                string_value += ", ";
                            }
                        }
                    }

                    auto result = string_builder->Append(string_value);
                    if (!result.ok()) {
                        return absl::Status(
                            absl::StatusCode::kInternal,
                            fmt::format("failed to append value, error {}",
                                        result.ToString()));
                    }
                    break;
                }
                default:
                    SPDLOG_ERROR("unsupported type: {}",
                                 small::type::to_string(column.type()));
                    return absl::Status(
                        absl::StatusCode::kInvalidArgument,
                        "unsupported type: " +
                            small::type::to_string(column.type()));
            }
        }
    }

    arrow::ArrayVector columns;
    for (const auto& builder : builders) {
        auto result = builder->Finish();
        if (!result.ok()) {
            return absl::Status(
                absl::StatusCode::kInternal,
                "Failed to finish builder: " + result.status().ToString());
        }
        const auto& column = result.ValueOrDie();
        columns.push_back(column);
    }

    int64_t num_records = columns[0]->length();

    SPDLOG_INFO("input_schema: {}", input_schema->ToString());
    SPDLOG_INFO("num_records: {}", num_records);
    SPDLOG_INFO("columns: {}", columns[0]->ToString());

    auto in_batch =
        arrow::RecordBatch::Make(input_schema, num_records, columns);

    std::vector<std::shared_ptr<arrow::Field>> output_fields;
    std::vector<std::shared_ptr<gandiva::Expression>> expressions;
    for (size_t t = 0; t < select_stmt->n_target_list; t++) {
        auto res_target = select_stmt->target_list[t]->res_target;
        auto val_node = res_target->val;

        if (val_node->node_case == PG_QUERY__NODE__NODE_COLUMN_REF) {
            auto column_ref = val_node->column_ref;
            for (size_t i = 0; i < column_ref->n_fields; i++) {
                auto field = column_ref->fields[i];
                switch (field->node_case) {
                    case PG_QUERY__NODE__NODE_A_STAR:
                        for (const auto& f : input_schema->fields()) {
                            auto node = gandiva::TreeExprBuilder::MakeField(f);
                            auto expression =
                                gandiva::TreeExprBuilder::MakeExpression(node,
                                                                         f);
                            expressions.push_back(expression);
                            output_fields.push_back(f);
                        }
                        break;
                    case PG_QUERY__NODE__NODE_STRING: {
                        auto col_name = std::string(field->string->sval);
                        auto f = input_schema->GetFieldByName(col_name);
                        if (f) {
                            auto node = gandiva::TreeExprBuilder::MakeField(f);
                            auto expression =
                                gandiva::TreeExprBuilder::MakeExpression(node,
                                                                         f);
                            expressions.push_back(expression);
                            output_fields.push_back(f);
                        }
                        break;
                    }
                    default:
                        SPDLOG_ERROR("unsupported field type");
                        return absl::Status(
                            absl::StatusCode::kInvalidArgument,
                            "unsupported field type: " +
                                std::string(
                                    magic_enum::enum_name(field->node_case)));
                }
            }
        }
    }

    gandiva::SchemaPtr output_schema = arrow::schema(output_fields);
    SPDLOG_INFO("output schema: {}", output_schema->ToString());

    // Gandiva's projector rejects zero-row batches with "RecordBatch must be
    // non-empty". An empty input trivially maps to an empty output under
    // projection, so short-circuit by reusing the already-empty input arrays.
    if (num_records == 0) {
        arrow::ArrayVector empty_outputs;
        empty_outputs.reserve(output_fields.size());
        for (const auto& f : output_fields) {
            int idx = input_schema->GetFieldIndex(f->name());
            if (idx < 0) {
                return absl::InternalError(fmt::format(
                    "output field {} not in input schema", f->name()));
            }
            empty_outputs.push_back(columns[idx]);
        }
        return arrow::RecordBatch::Make(output_schema, 0, empty_outputs);
    }

    std::shared_ptr<gandiva::Projector> projector;
    arrow::Status status;
    status = gandiva::Projector::Make(input_schema, expressions, &projector);
    if (!status.ok()) {
        SPDLOG_ERROR("projector make failed: {}", status.ToString());
        return absl::Status(absl::StatusCode::kInternal,
                            "projector make failed: " + status.ToString());
    }

    auto pool = arrow::default_memory_pool();

    SPDLOG_INFO("in_batch: {}", in_batch->ToString());
    SPDLOG_INFO("input_schema: {}", input_schema->ToString());

    arrow::ArrayVector outputs;
    status = projector->Evaluate(*in_batch, pool, &outputs);
    if (!status.ok()) {
        SPDLOG_ERROR("projector evaluate failed: {}", status.ToString());
        return absl::Status(absl::StatusCode::kInternal,
                            "projector evaluate failed: " + status.ToString());
    }
    std::shared_ptr<arrow::RecordBatch> result =
        arrow::RecordBatch::Make(output_schema, outputs[0]->length(), outputs);

    SPDLOG_INFO("project result: {}", result->ToString());

    return result;
}

grpc::Status QueryServiceImpl::Query(
    grpc::ServerContext* context, const small::execution::RawNode* request,
    small::execution::QueryResponse* response) {
    SPDLOG_INFO("query request: {} bytes", request->packed_node().size());

    PgQuery__SelectStmt* node = pg_query__select_stmt__unpack(
        nullptr, request->packed_node().size(),
        reinterpret_cast<const uint8_t*>(request->packed_node().data()));

    auto result = query(node, /*dispatch=*/false, request->ts());
    pg_query__select_stmt__free_unpacked(node, nullptr);

    if (!result.ok()) {
        return {grpc::StatusCode::INTERNAL,
                std::string(result.status().message())};
    }
    auto batch = result.value();

    auto sink_result = arrow::io::BufferOutputStream::Create();
    if (!sink_result.ok()) {
        return {grpc::StatusCode::INTERNAL, sink_result.status().ToString()};
    }
    auto sink = sink_result.ValueOrDie();
    auto writer_result = arrow::ipc::MakeStreamWriter(sink, batch->schema());
    if (!writer_result.ok()) {
        return {grpc::StatusCode::INTERNAL, writer_result.status().ToString()};
    }
    auto writer = writer_result.ValueOrDie();
    auto write_status = writer->WriteRecordBatch(*batch);
    if (!write_status.ok()) {
        return {grpc::StatusCode::INTERNAL, write_status.ToString()};
    }
    auto close_status = writer->Close();
    if (!close_status.ok()) {
        return {grpc::StatusCode::INTERNAL, close_status.ToString()};
    }

    auto buffer_result = sink->Finish();
    if (!buffer_result.ok()) {
        return {grpc::StatusCode::INTERNAL, buffer_result.status().ToString()};
    }
    auto buffer = buffer_result.ValueOrDie();
    response->set_ipc_bytes(buffer->data(), buffer->size());
    return grpc::Status::OK;
}

}  // namespace small::execution
