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
#include <unordered_map>
#include <utility>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

// spdlog
#include "spdlog/spdlog.h"

// json
#include "nlohmann/json.hpp"

// =====================================================================
// local libraries
// =====================================================================

#include "src/server_info/info.h"

// =====================================================================
// self header
// =====================================================================

#include "src/catalog/catalog.h"

namespace small::catalog {

Catalog* Catalog::instancePtr = nullptr;

void Catalog::InitInstance() {
    if (instancePtr == nullptr) {
        instancePtr = new Catalog();
    } else {
        SPDLOG_ERROR("catalog instance already initialized");
    }
}

Catalog* Catalog::GetInstance() {
    if (instancePtr == nullptr) {
        SPDLOG_ERROR("catalog instance not initialized");
        return nullptr;
    }
    return instancePtr;
}

Catalog::Catalog() {
    std::vector<small::schema::Column> columns;
    columns.emplace_back("table_name", small::type::Type::String, true);
    columns.emplace_back("columns", small::type::Type::String);
    this->tables["system.tables"] =
        std::make_shared<small::schema::Table>("system.tables", columns);
    this->system_tables = this->tables["system.tables"];

    columns.clear();
    columns.emplace_back("table_name", small::type::Type::String);
    columns.emplace_back("partition_name", small::type::Type::String, true);
    columns.emplace_back("constraint", small::type::Type::String);
    columns.emplace_back("column_name", small::type::Type::String);
    columns.emplace_back("partition_value", small::type::Type::String);
    this->tables["system.partitions"] =
        std::make_shared<small::schema::Table>("system.partitions", columns);
    this->system_partitions = this->tables["system.partitions"];

    auto info = small::server_info::get_info();
    if (!info.ok()) {
        SPDLOG_ERROR("failed to get server info");
        return;
    }
    std::string db_path = info.value()->db_path;
    this->db = small::rocks::RocksDBWrapper::GetInstance(
        db_path, {"TablesCF", "PartitionCF"});
}

std::optional<std::shared_ptr<small::schema::Table>> Catalog::GetTable(
    const std::string& table_name) {
    auto it = tables.find(table_name);
    if (it != tables.end()) {
        return it->second;
    } else {
        return std::nullopt;
    }
}

absl::Status Catalog::CreateTable(
    const std::string& table_name,
    const std::vector<small::schema::Column>& columns) {
    auto table = GetTable(table_name);
    if (table.has_value()) {
        return absl::AlreadyExistsError("Table already exists");
    }

    // write to in-memory cache
    auto new_table =
        std::make_shared<small::schema::Table>(table_name, columns);
    tables[table_name] = new_table;

    // write to disk
    std::vector<small::type::Datum> row;
    row.emplace_back(table_name);
    row.emplace_back(nlohmann::json(columns).dump());

    db->WriteRow(this->system_tables, row);

    return absl::OkStatus();
}

absl::Status Catalog::DropTable(const std::string& table_name) {
    auto it = tables.find(table_name);
    if (it != tables.end()) {
        tables.erase(it);
    }

    db->Delete("TablesCF", table_name);
    return absl::OkStatus();
}

absl::Status Catalog::SetPartition(const std::string& table_name,
                                   const std::string& partition_column,
                                   PgQuery__PartitionStrategy strategy) {
    switch (strategy) {
        case PG_QUERY__PARTITION_STRATEGY__PARTITION_STRATEGY_LIST: {
            auto p = small::schema::ListPartition(partition_column);
            auto table = GetTable(table_name);
            if (!table.has_value()) {
                return absl::NotFoundError("Table not found");
            }

            // write to in-memory cache
            this->parititions[table_name] =
                std::make_shared<small::schema::partition_t>(p);
            table.value()->partition = p;

            // write to disk
            WritePartition(table.value());

            return absl::OkStatus();
        }

        default: {
            return absl::InternalError(
                "Unsupported partition strategy: " +
                std::to_string(static_cast<int>(strategy)));
        }
    }

    return absl::OkStatus();
}
void Catalog::WritePartition(
    const std::shared_ptr<small::schema::Table>& table) {
    std::visit(
        [&](auto&& partition) {
            using T = std::decay_t<decltype(partition)>;

            if constexpr (std::is_same_v<T, small::schema::ListPartition>) {
                for (auto& [p_name, p] : partition.partitions) {
                    std::vector<small::type::Datum> row;
                    row.emplace_back(table->name);
                    row.emplace_back(p_name);
                    row.emplace_back(nlohmann::json(p.constraints).dump());
                    row.emplace_back(partition.column_name);
                    row.emplace_back(nlohmann::json(p.values).dump());
                    db->WriteRow(this->system_partitions, row);
                }
            } else {
                SPDLOG_ERROR("unsupported partition type: {}",
                             typeid(T).name());
            }
        },
        table->partition);
}

absl::Status Catalog::AddListPartition(const std::string& table_name,
                                       const std::string& partition_name,
                                       const std::vector<std::string>& values) {
    for (const auto& [table_name, table] : tables) {
        if (auto* listP =
                std::get_if<small::schema::ListPartition>(&table->partition)) {
            listP->partitions[partition_name] =
                small::schema::ListPartition::SinglePartition{values, {}};
            WritePartition(table);
            return absl::OkStatus();
        }
    }
    return absl::NotFoundError("table not found");
}

absl::Status Catalog::AddPartitionConstraint(
    const std::string& partition_name,
    const std::pair<std::string, std::string>& constraint) {
    for (const auto& [table_name, table] : tables) {
        if (auto* listP =
                std::get_if<small::schema::ListPartition>(&table->partition)) {
            auto it = listP->partitions.find(partition_name);
            if (it != listP->partitions.end()) {
                auto& p = it->second;
                p.constraints.insert(constraint);
                WritePartition(table);
                return absl::OkStatus();
            }
        }
    }
    return absl::NotFoundError("Partition not found");
}

}  // namespace small::catalog
