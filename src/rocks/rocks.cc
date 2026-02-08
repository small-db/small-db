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

#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

// rocksdb
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"

// absl
#include "absl/strings/str_format.h"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/server_info/info.h"

// =====================================================================
// self header
// =====================================================================

#include "src/rocks/rocks.h"

namespace small::rocks {

absl::StatusOr<RocksDBWrapper*> RocksDBWrapper::GetInstance() {
    auto info = small::server_info::get_info();
    if (!info.ok()) return absl::InternalError("failed to get server info");
    std::string db_path = info.value()->db_path;
    return small::rocks::RocksDBWrapper::GetInstance(db_path);
}

RocksDBWrapper* RocksDBWrapper::GetInstance(const std::string& db_path) {
    static std::unordered_map<std::string, RocksDBWrapper*> instances;
    auto it = instances.find(db_path);
    if (it != instances.end()) {
        return it->second;
    }

    // Create a new instance if it doesn't exist
    instances[db_path] = new RocksDBWrapper(db_path);
    return instances[db_path];
}

RocksDBWrapper::RocksDBWrapper(const std::string& db_path) {
    bool _ = std::filesystem::create_directories(db_path);

    rocksdb::Options options;
    options.create_if_missing = true;

    // Open database with column families
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db_);
    if (!status.ok()) {
        throw std::runtime_error("Failed to open RocksDB: " +
                                 status.ToString());
    }
}

RocksDBWrapper::~RocksDBWrapper() { Close(); }

void RocksDBWrapper::Close() { delete db_; }

bool RocksDBWrapper::Put(const std::string& key, const std::string& value) {
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, value);
    return status.ok();
}

bool RocksDBWrapper::Get(const std::string& key, std::string& value) {
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);
    return status.ok();
}

std::map<std::string, std::map<std::string, std::string>>
RocksDBWrapper::ReadTable(const std::string& table_name) {
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;

    auto scan_prefix = "/" + table_name + "/";

    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options));

    // Result structure: {primary_key -> {column_name -> value}}
    std::map<std::string, std::map<std::string, std::string>> result;

    for (it->Seek(scan_prefix);
         it->Valid() && it->key().starts_with(scan_prefix); it->Next()) {
        std::string key = it->key().ToString();
        std::string value = it->value().ToString();

        // Parse key format: "/{table_name}/{pk}/{column_name}"
        // Skip the leading "/" and table_name + "/"
        size_t start_pos = scan_prefix.length();
        size_t pk_end = key.find('/', start_pos);
        if (pk_end != std::string::npos) {
            std::string pk = key.substr(start_pos, pk_end - start_pos);
            std::string column_name = key.substr(pk_end + 1);

            // Add to result structure
            result[pk][column_name] = value;
        }
    }

    return result;
}

bool RocksDBWrapper::Delete(const std::string& key) {
    rocksdb::Status status = db_->Delete(rocksdb::WriteOptions(), key);
    return status.ok();
}

void RocksDBWrapper::PrintAllKV() {
    rocksdb::ReadOptions read_options;
    rocksdb::Iterator* it = db_->NewIterator(read_options);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::cout << "\tKey: " << it->key().ToString()
                  << ", Value: " << it->value().ToString() << std::endl;
    }
    delete it;
}

void RocksDBWrapper::WriteRow(
    const std::shared_ptr<small::schema::Table>& table, const std::string& pk,
    const std::vector<std::string>& values) {
    for (int i = 0; i < table->columns().size(); ++i) {
        const auto& column = table->columns()[i];
        auto key =
            absl::StrFormat("/%s/%s/%s", table->name(), pk, column.name());
        this->Put(key, values[i]);
    }
}

void RocksDBWrapper::WriteCell(
    const std::shared_ptr<small::schema::Table>& table, const std::string& pk,
    const std::string& column_name, const std::string& value) {
    auto key = absl::StrFormat("/%s/%s/%s", table->name(), pk, column_name);
    this->Put(key, value);
}

}  // namespace small::rocks
