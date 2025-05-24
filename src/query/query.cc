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

#include <iostream>
#include <memory>
#include <string>
#include <tuple>
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
#include "arrow/compute/api_vector.h"
#include "arrow/status.h"

// arrow gandiva
#include "gandiva/filter.h"
#include "gandiva/projector.h"
#include "gandiva/selection_vector.h"
#include "gandiva/tree_expr_builder.h"

// magic_enum
#include "magic_enum/magic_enum.hpp"

// =====================================================================
// local libraries
// =====================================================================

#include "src/catalog/catalog.h"
#include "src/rocks/rocks.h"
#include "src/schema/const.h"
#include "src/schema/schema.h"
#include "src/server_info/info.h"

// =====================================================================
// self header
// =====================================================================

#include "src/query/query.h"

namespace query {

// parse key from rocksdb, the format is:
// /<table_name>/<pk>
std::tuple<std::string_view, std::string_view> parse_key(
    const std::string& key) {
    size_t first_slash = key.find('/');
    if (first_slash == std::string::npos) {
        throw std::invalid_argument("Invalid key format: missing first slash");
    }

    size_t second_slash = key.find('/', first_slash + 1);
    if (second_slash == std::string::npos) {
        throw std::invalid_argument("Invalid key format: missing second slash");
    }

    std::string_view table_name = std::string_view(key).substr(
        first_slash + 1, second_slash - first_slash - 1);
    std::string_view pk = std::string_view(key).substr(second_slash + 1);
    return {table_name, pk};
}

std::shared_ptr<arrow::Schema> get_input_schema(
    const small::schema::Table& table) {
    arrow::FieldVector fields;
    for (const auto& column : table.columns()) {
        fields.push_back(arrow::field(
            column.name(), small::type::get_gandiva_type(column.type())));
    }
    return arrow::schema(fields);
}

std::unordered_map<std::string, std::shared_ptr<arrow::ArrayBuilder>>
get_builders(const small::schema::Table& table) {
    std::unordered_map<std::string, std::shared_ptr<arrow::ArrayBuilder>>
        builders;
    for (const auto& column : table.columns()) {
        switch (column.type()) {
            case small::type::Type::INT64:
                builders[column.name()] =
                    std::make_shared<arrow::Int64Builder>();
                break;
            case small::type::Type::STRING:
                builders[column.name()] =
                    std::make_shared<arrow::StringBuilder>();
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
    PgQuery__SelectStmt* select_stmt) {
    auto schemaname = select_stmt->from_clause[0]->range_var->schemaname;
    auto relname = select_stmt->from_clause[0]->range_var->relname;

    auto table_name = std::string(schemaname) + "." + std::string(relname);

    // get the input schema
    auto table =
        small::catalog::CatalogManager::GetInstance()->GetTable(table_name);
    if (!table) {
        SPDLOG_ERROR("table not found: {}", table_name);
        return absl::Status(absl::StatusCode::kNotFound,
                            "table not found: " + table_name);
    }
    auto input_schema = get_input_schema(*table.value());
    SPDLOG_INFO("schema: {}", input_schema->ToString());

    // read kv pairs from rocksdb
    auto info = small::server_info::get_info();
    if (!info.ok())
        return absl::Status(absl::StatusCode::kInternal,
                            "failed to get server info");
    std::string db_path = info.value()->db_path;
    auto db = small::rocks::RocksDBWrapper::GetInstance(db_path, {});
    auto scan_preix = "/" + table_name + "/";
    auto kv_pairs = db->GetAll(scan_preix);

    // init builders
    auto builders = get_builders(*table.value());

    for (const auto& [key, value] : kv_pairs) {
        SPDLOG_INFO("key: {}, value: {}", key, value);

        nlohmann::json parsed = nlohmann::json::parse(value);

        auto [_, _] = parse_key(key);

        for (const auto& column : table.value()->columns()) {
            // ensure the builder is valid
            auto builder = builders[column.name()];
            if (builder == nullptr) {
                return absl::Status(
                    absl::StatusCode::kInternal,
                    "builder is null for column: " + column.name());
            }

            // ensure the value is valid
            if (!parsed.contains(column.name())) {
                return absl::Status(absl::StatusCode::kInvalidArgument,
                                    "column not found in json");
            }

            switch (column.type()) {
                case small::type::Type::INT64: {
                    auto int_builder =
                        std::dynamic_pointer_cast<arrow::Int64Builder>(builder);
                    int64_t int_value = parsed[column.name()];
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
                    std::string string_value = parsed[column.name()];
                    auto result = string_builder->Append(string_value);
                    if (!result.ok()) {
                        return absl::Status(
                            absl::StatusCode::kInternal,
                            fmt::format("failed to append value, error {}",
                                        result.ToString()));
                        break;
                    }
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
    for (const auto& [_, builder] : builders) {
        auto result = builder->Finish();
        if (!result.ok()) {
            return absl::Status(
                absl::StatusCode::kInternal,
                "Failed to finish builder: " + result.status().ToString());
        }
        auto column = result.ValueOrDie();
        columns.push_back(column);
    }

    int num_records = columns[0]->length();

    auto in_batch =
        arrow::RecordBatch::Make(input_schema, num_records, columns);

    std::vector<std::shared_ptr<arrow::Field>> output_fields;

    // get result schema
    std::vector<std::shared_ptr<gandiva::Expression>> expressions;
    auto column_ref = select_stmt->target_list[0]->res_target->val->column_ref;
    for (int i = 0; i < column_ref->n_fields; i++) {
        auto field = column_ref->fields[i];
        switch (field->node_case) {
            case PG_QUERY__NODE__NODE_A_STAR:
                for (auto field : input_schema->fields()) {
                    auto column_ref =
                        gandiva::TreeExprBuilder::MakeField(field);
                    auto expression = gandiva::TreeExprBuilder::MakeExpression(
                        column_ref, field);
                    expressions.push_back(expression);

                    output_fields.push_back(field);
                }
                break;
            default:
                SPDLOG_ERROR("unsupported field type");
                return absl::Status(
                    absl::StatusCode::kInvalidArgument,
                    "unsupported field type: " +
                        std::string(magic_enum::enum_name(field->node_case)));
        }
    }

    gandiva::SchemaPtr output_schema = arrow::schema(output_fields);
    SPDLOG_INFO("output schema: {}", output_schema->ToString());

    std::shared_ptr<gandiva::Projector> projector;
    arrow::Status status;
    status = gandiva::Projector::Make(input_schema, expressions, &projector);
    if (!status.ok()) {
        SPDLOG_ERROR("projector make failed: {}", status.ToString());
        return absl::Status(absl::StatusCode::kInternal,
                            "projector make failed: " + status.ToString());
    }

    auto pool = arrow::default_memory_pool();
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

}  // namespace query
