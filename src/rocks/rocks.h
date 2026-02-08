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

#include <map>
#include <memory>
#include <string>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

#include "absl/status/statusor.h"
#include "rocksdb/db.h"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/schema/schema.pb.h"

namespace small::rocks {

class RocksDBWrapper {
   private:
    // singleton instance
    explicit RocksDBWrapper(const std::string& db_path);
    ~RocksDBWrapper();

   public:
    // Get db instance of the current server process.
    static absl::StatusOr<RocksDBWrapper*> GetInstance();

    // Get db instance of the specified path.
    static RocksDBWrapper* GetInstance(const std::string& db_path);

    // copy blocker
    RocksDBWrapper(const RocksDBWrapper&) = delete;

    // assignment blocker
    void operator=(const RocksDBWrapper&) = delete;

    bool Put(const std::string& key, const std::string& value);

    bool Get(const std::string& key, std::string& value);

    /**
     * @brief Retrieves all rows from a table
     *
     * @param table_name Name of the table to read
     * @return Map structure: {primary_key -> {column_name -> value}}
     */
    std::map<std::string, std::map<std::string, std::string>> ReadTable(
        const std::string& table_name);

    std::vector<std::pair<std::string, std::string>> GetAllKV();

    bool Delete(const std::string& key);

    void PrintAllKV();

    void WriteRow(const std::shared_ptr<small::schema::Table>& table,
                  const std::string& pk,
                  const std::vector<std::string>& values);

    void WriteCell(const std::shared_ptr<small::schema::Table>& table,
                   const std::string& pk, const std::string& column_name,
                   const std::string& value);

   private:
    rocksdb::DB* db_ = nullptr;

    void Close();
};

}  // namespace small::rocks
