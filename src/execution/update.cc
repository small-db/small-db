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
#include <string>

// =====================================================================
// third-party libraries
// =====================================================================

// pg_query
#include "pg_query.pb-c.h"

// absl
#include "absl/status/statusor.h"

// arrow
#include "arrow/api.h"

// spdlog
#include "spdlog/spdlog.h"

// nlohmann/json
#include "nlohmann/json.hpp"

// =====================================================================
// local libraries
// =====================================================================

#include "src/catalog/catalog.h"
#include "src/rocks/rocks.h"
#include "src/semantics/extract.h"
#include "src/type/type.h"

// =====================================================================
// self header
// =====================================================================

#include "src/execution/update.h"

namespace query {

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> update(
    PgQuery__UpdateStmt* update_stmt) {
    auto table_name = update_stmt->relation->relname;
    auto result =
        small::catalog::CatalogManager::GetInstance()->GetTable(table_name);
    if (!result) {
        return absl::InternalError(
            fmt::format("table {} not found", table_name));
    }
    const auto& table = result.value();

    auto db = small::rocks::RocksDBWrapper::GetInstance().value();
    auto rows = db->ReadTable(table_name);

    // print rows
    for (const auto& [pk, columns] : rows) {
        SPDLOG_INFO("pk: {}, columns: {}", pk, nlohmann::json(columns).dump());
    }

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
        auto val = columns.at(filter_column);
        if (val == encoded_filter_value) {
            filtered_rows[pk][filter_column] = val;
        }
    }

    // print filtered rows
    for (const auto& [pk, columns] : filtered_rows) {
        SPDLOG_INFO("filtered pk: {}, columns: {}", pk,
                    nlohmann::json(columns).dump());
    }

    // db->WriteCell(table, pk, column_name, value);

    return absl::Status(absl::StatusCode::kInternal,
                        "unimplemented update executor");
}

}  // namespace query
