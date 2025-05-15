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

#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

#include "rocksdb/db.h"
#include "rocksdb/options.h"

// =====================================================================
// local libraries
// =====================================================================

#include "src/encode/encode.h"
#include "src/schema/schema.h"
#include "src/type/type.h"

namespace small::rocks {

class RocksDBWrapper {
   private:
    // singleton instance
    RocksDBWrapper(const std::string& db_path,
                   const std::vector<std::string>& column_family_names);
    ~RocksDBWrapper();

   public:
    static RocksDBWrapper* GetInstance(
        const std::string& db_path,
        const std::vector<std::string>& column_family_names) {
        static std::unordered_map<std::string, RocksDBWrapper*> instances;
        auto it = instances.find(db_path);
        if (it != instances.end()) {
            return it->second;
        }

        // Create a new instance if it doesn't exist
        instances[db_path] = new RocksDBWrapper(db_path, column_family_names);
        return instances[db_path];
    }

    // copy blocker
    RocksDBWrapper(const RocksDBWrapper&) = delete;

    // assignment blocker
    void operator=(const RocksDBWrapper&) = delete;

    bool Put(const std::string& key, const std::string& value);
    bool Put(const std::string& cf_name, const std::string& key,
             const std::string& value);

    bool Get(const std::string& key, std::string& value);
    bool Get(const std::string& cf_name, const std::string& key,
             std::string& value);

    std::vector<std::pair<std::string, std::string>> GetAll(
        const std::string& prefix);
    std::vector<std::pair<std::string, std::string>> GetAllKV(
        const std::string& cf_name);

    bool Delete(const std::string& cf_name, const std::string& key);

    void PrintAllKV();

    void WriteRow(const std::shared_ptr<small::schema::Table>& table,
                  const std::vector<small::type::Datum>& values);

    void WriteRowWire(const std::shared_ptr<small::schema::Table>& table,
                      const std::vector<std::string>& values);

   private:
    rocksdb::DB* db_;
    std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*> cf_handles_;

    void Close();
    rocksdb::ColumnFamilyHandle* GetColumnFamilyHandle(
        const std::string& cf_name);
};

}  // namespace small::rocks
