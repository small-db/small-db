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

// protobuf
#include "google/protobuf/util/json_util.h"

// =====================================================================
// local libraries
// =====================================================================

#include "src/gossip/gossip.h"
#include "src/server_info/info.h"
#include "src/schema/schema.h"

// =====================================================================
// self header
// =====================================================================

#include "src/catalog/catalog.h"

namespace small::catalog {

CatalogManager* CatalogManager::instancePtr = nullptr;

void CatalogManager::InitInstance() {
    if (instancePtr == nullptr) {
        instancePtr = new CatalogManager();
    } else {
        SPDLOG_ERROR("catalog instance already initialized");
    }
}

CatalogManager* CatalogManager::GetInstance() {
    if (instancePtr == nullptr) {
        SPDLOG_ERROR("catalog instance not initialized");
        return nullptr;
    }
    return instancePtr;
}

CatalogManager::CatalogManager() {
    // init table "system.tables"
    {
        auto system_tables = std::make_shared<small::schema::Table>();
        this->tables["system.tables"] = system_tables;
        this->system_tables = system_tables;

        system_tables->set_name("system.tables");

        auto column = system_tables->add_columns();
        column->set_name("table_name");
        column->set_type(small::type::Type::STRING);
        column->set_is_primary_key(true);

        column = system_tables->add_columns();
        column->set_name("columns");
        column->set_type(small::type::Type::STRING);
    }

    {
        auto system_partitions = std::make_shared<small::schema::Table>();
        this->tables["system.partitions"] = system_partitions;
        this->system_partitions = system_partitions;

        system_partitions->set_name("system.partitions");

        auto column = system_partitions->add_columns();
        column->set_name("table_name");
        column->set_type(small::type::Type::STRING);

        column = system_partitions->add_columns();
        column->set_name("partition_name");
        column->set_type(small::type::Type::STRING);
        column->set_is_primary_key(true);

        column = system_partitions->add_columns();
        column->set_name("constraint");
        column->set_type(small::type::Type::STRING);

        column = system_partitions->add_columns();
        column->set_name("column_name");
        column->set_type(small::type::Type::STRING);

        column = system_partitions->add_columns();
        column->set_name("partition_value");
        column->set_type(small::type::Type::STRING);
    }

    auto info = small::server_info::get_info();
    if (!info.ok()) {
        SPDLOG_ERROR("failed to get server info");
        return;
    }
    std::string db_path = info.value()->db_path;
    this->db = small::rocks::RocksDBWrapper::GetInstance(db_path, {});
}

std::optional<std::shared_ptr<small::schema::Table>> CatalogManager::GetTable(
    const std::string& table_name) {
    auto it = tables.find(table_name);
    if (it != tables.end()) {
        return it->second;
    } else {
        return std::nullopt;
    }
}

absl::Status CatalogManager::CreateTable(
    const std::string& table_name,
    const std::vector<small::schema::Column>& columns) {
    // update local catalog
    auto status = CreateTableLocal(table_name, columns);
    if (!status.ok()) {
        SPDLOG_ERROR("create table failed: {}", status.ToString());
        return status;
    }

    auto nodes = small::gossip::get_nodes();
    SPDLOG_INFO("nodes size: {}", nodes.size());
    for (const auto& [_, node] : nodes) {
        SPDLOG_INFO("node: {}", node.sql_addr);
    }
    if (nodes.size() != 3) {
        return absl::InternalError("not enough nodes");
    }

    return absl::OkStatus();
}

absl::Status CatalogManager::CreateTableLocal(
    const std::string& table_name,
    const std::vector<small::schema::Column>& columns) {
    if (GetTable(table_name).has_value()) {
        return absl::AlreadyExistsError("Table already exists");
    }

    auto table = std::make_shared<small::schema::Table>();
    table->set_name(table_name);
    for (const auto& column : columns) {
        table->add_columns()->CopyFrom(column);
    }

    return UpdateTable(table);
}

absl::Status CatalogManager::UpdateTable(
    const std::shared_ptr<small::schema::Table>& table) {
    // write to in-memory cache
    tables[table->name()] = table;

    // write to disk
    {
        std::vector<std::string> values;

        // name
        values.push_back(table->name());

        // columns
        values.push_back(nlohmann::json(table->columns()).dump());

        db->WriteRow(this->system_tables, table->name(), values);

        // partition
        if (table->has_partition()) {
            auto partition = table->partition();
            if (partition.has_list_partition()) {
                auto list_partition = partition.list_partition();
                for (const auto& [partition_name, partition_item] :
                     list_partition.partitions()) {
                    std::vector<std::string> values;

                    // table_name
                    values.push_back(table->name());

                    // partition_name
                    values.push_back(partition_name);

                    // constraint
                    values.push_back(
                        nlohmann::json(partition_item.constraints()).dump());

                    // column_name
                    values.push_back(list_partition.column_name());

                    // partition values
                    values.push_back(
                        nlohmann::json(partition_item.values()).dump());

                    db->WriteRow(this->system_partitions, partition_name,
                                 values);
                }
            }
        }
    }
    return absl::OkStatus();
}

absl::Status CatalogManager::DropTable(const std::string& table_name) {
    auto it = tables.find(table_name);
    if (it != tables.end()) {
        tables.erase(it);
    }

    db->Delete(table_name);
    return absl::OkStatus();
}

absl::Status CatalogManager::SetPartition(const std::string& table_name,
                                          const std::string& partition_column,
                                          PgQuery__PartitionStrategy strategy) {
    switch (strategy) {
        case PG_QUERY__PARTITION_STRATEGY__PARTITION_STRATEGY_LIST: {
            auto table = GetTable(table_name);
            if (!table.has_value()) {
                return absl::NotFoundError("Table not found");
            }

            auto list_partition =
                table.value()->mutable_partition()->mutable_list_partition();
            list_partition->set_column_name(partition_column);

            return UpdateTable(table.value());
        }

        default: {
            return absl::InternalError(
                "Unsupported partition strategy: " +
                std::to_string(static_cast<int>(strategy)));
        }
    }
}

absl::Status CatalogManager::ListPartitionAddValues(
    const std::string& table_name, const std::string& partition_name,
    const std::vector<std::string>& values) {
    auto it = tables.find(table_name);
    if (it == tables.end()) {
        return absl::NotFoundError("table not found");
    }

    auto& table = it->second;
    auto* list_partition = table->mutable_partition()->mutable_list_partition();
    auto* partition_item =
        &(*list_partition->mutable_partitions())[partition_name];
    for (const auto& v : values) {
        partition_item->add_values(v);
    }
    return UpdateTable(table);
}

absl::Status CatalogManager::ListPartitionAddConstraint(
    const std::string& partition_name,
    const std::pair<std::string, std::string>& new_constraint) {
    for (auto& [table_name, table] : tables) {
        auto partition = table->partition();
        if (partition.has_list_partition()) {
            auto partitions = table->mutable_partition()
                                  ->mutable_list_partition()
                                  ->mutable_partitions();
            auto partition_it = partitions->find(partition_name);
            if (partition_it != partitions->end()) {
                auto& partition_item = partition_it->second;
                auto* constraints = partition_item.mutable_constraints();
                constraints->insert(new_constraint);
                return UpdateTable(table);
            }
        }
    }

    return absl::NotFoundError("parition not found");
}

grpc::Status CatalogService::CreateTable(
    grpc::ServerContext* context,
    const small::catalog::CreateTableRequest* request,
    small::catalog::CreateTableReply* response) {
    SPDLOG_INFO("create table request: {}", request->DebugString());
    return grpc::Status::OK;
}

}  // namespace small::catalog
