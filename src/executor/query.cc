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
#include <tuple>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

// pg_query
#include "pg_query.pb-c.h"

// spdlog
#include "spdlog/spdlog.h"

// arrow
#include "arrow/api.h"
#include "arrow/status.h"

// arrow gandiva
#include "gandiva/projector.h"
#include "gandiva/tree_expr_builder.h"

// magic_enum
#include "magic_enum/magic_enum.hpp"

// =====================================================================
// local libraries
// =====================================================================

#include "src/catalog/catalog.h"
#include "src/rocks/rocks.h"
#include "src/server_info/info.h"
#include "src/type/type.h"

// =====================================================================
// self header
// =====================================================================

#include "src/executor/query.h"

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

/**
 * @brief Get the builders object.
 *
 * @param table
 * @return std::vector<std::shared_ptr<arrow::ArrayBuilder>> -
 *         order by column order in table
 */
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
    auto db = small::rocks::RocksDBWrapper::GetInstance(db_path);
    auto rows = db->ReadTable(table_name);

    // init builders
    auto builders = get_builders(table.value());

    for (const auto& [pk, columns] : rows) {
        SPDLOG_INFO("pk: {}, columns: {}", pk, nlohmann::json(columns).dump());

        for (const auto& column : table.value()->columns()) {
            SPDLOG_INFO("column: {}", column.name());
        }

        for (int i = 0; i < table.value()->columns().size(); i++) {
            const auto& column = table.value()->columns()[i];
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
                        // dedicate branch to modify the value for "columns"
                        // column
                        //
                        // TODO: generalize this logic

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

                            // name
                            string_value += col.name();

                            // type
                            string_value += ":";
                            string_value += small::type::to_string(col.type());

                            // is_primary_key
                            if (col.is_primary_key()) {
                                string_value += "(PK)";
                            }

                            // comma
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
        // SPDLOG_INFO("column_name: {}", column_name);
        auto result = builder->Finish();
        if (!result.ok()) {
            return absl::Status(
                absl::StatusCode::kInternal,
                "Failed to finish builder: " + result.status().ToString());
        }
        const auto& column = result.ValueOrDie();
        columns.push_back(column);
    }

    int num_records = columns[0]->length();

    SPDLOG_INFO("input_schema: {}", input_schema->ToString());
    SPDLOG_INFO("num_records: {}", num_records);
    SPDLOG_INFO("columns: {}", columns[0]->ToString());

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

    // log in_batch and input_schema
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

}  // namespace query
