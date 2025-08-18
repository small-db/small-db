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

#pragma once

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

// pg_query
#include "pg_query.pb-c.h"

// =====================================================================
// local libraries
// =====================================================================

#include "src/rocks/rocks.h"
#include "src/schema/schema.h"

// =====================================================================
// protobuf generated files
// =====================================================================

#include "catalog.grpc.pb.h"
#include "catalog.pb.h"

namespace small::catalog {

class CatalogManager {
   private:
    // singleton instance - the only instance
    static CatalogManager* instancePtr;

    // singleton instance - constructor protector
    CatalogManager();

    // singleton instance - destructor protector
    ~CatalogManager() = default;

    small::rocks::RocksDBWrapper* db;

    // all tables, key: table_name, value: table
    std::unordered_map<std::string, std::shared_ptr<small::schema::Table>>
        tables;

    // all partitions, key: partition_name, value: partition
    std::unordered_map<std::string, std::shared_ptr<small::schema::Partition>>
        partitions;

    // built-in tables
    std::shared_ptr<small::schema::Table> system_tables;
    std::shared_ptr<small::schema::Table> system_partitions;

   public:
    // singleton instance - assignment-blocker
    void operator=(const CatalogManager&) = delete;

    // singleton instance - copy-blocker
    CatalogManager(const CatalogManager&) = delete;

    // singleton instance - get api
    static CatalogManager* GetInstance();

    // singleton instance - init api
    static void InitInstance();

    absl::Status CreateTable(const std::string& table_name,
                             const std::vector<small::schema::Column>& columns);

    absl::Status CreateTableLocal(
        const std::string& table_name,
        const std::vector<small::schema::Column>& columns);

    /*
     * Update the table metadata, create new table if not exists. If broadcast
     * is true, the update will be sent to all other servers.
     */
    absl::Status UpdateTable(const std::shared_ptr<small::schema::Table>& table,
                             bool broadcast);

    absl::Status DropTable(const std::string& table_name);

    std::optional<std::shared_ptr<small::schema::Table>> GetTable(
        const std::string& table_name);

    absl::Status SetPartition(const std::string& table_name,
                              const std::string& partition_column,
                              PgQuery__PartitionStrategy strategy);

    absl::Status ListPartitionAddValues(const std::string& table_name,
                                        const std::string& partition_name,
                                        const std::vector<std::string>& values);

    absl::Status ListPartitionAddConstraint(
        const std::string& partition_name,
        const std::pair<std::string, std::string>& new_constraint);
};

class CatalogServiceImpl final : public small::catalog::Catalog::Service {
   public:
    grpc::Status CreateTable(grpc::ServerContext* context,
                             const small::catalog::CreateTableRequest* request,
                             small::catalog::Reply* response) final;

   public:
    grpc::Status UpdateTable(grpc::ServerContext* context,
                             const small::schema::Table* request,
                             small::catalog::Reply* response) final;
};

}  // namespace small::catalog
