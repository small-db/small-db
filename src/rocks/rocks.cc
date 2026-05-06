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

#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
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

// nlohmann/json
#include "nlohmann/json.hpp"

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

std::map<std::string, std::map<std::string, std::string>>
RocksDBWrapper::ReadTable(const std::string& table_name, int64_t snapshot_ts) {
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

        // Parse key format: "/{table_name}/{pk}/{timestamp}"
        size_t start_pos = scan_prefix.length();
        size_t pk_end = key.find('/', start_pos);
        if (pk_end == std::string::npos) {
            continue;
        }
        std::string pk = key.substr(start_pos, pk_end - start_pos);

        // The third segment is the 20-digit zero-padded ms timestamp.
        std::string ts_str = key.substr(pk_end + 1);
        int64_t version_ts = std::stoll(ts_str);

        // Skip versions that are not yet visible at the requested snapshot.
        if (version_ts > snapshot_ts) {
            continue;
        }

        // Lexicographic order on zero-padded timestamps means the last
        // visible value we see for a given pk is the most recent version
        // at or before snapshot_ts.
        auto columns = nlohmann::json::parse(value);
        result[pk] = columns.get<std::map<std::string, std::string>>();
    }

    return result;
}

std::optional<std::map<std::string, std::string>> RocksDBWrapper::ReadLatest(
    const std::string& table_name, const std::string& pk) {
    auto scan_prefix = "/" + table_name + "/" + pk + "/";

    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options));

    // Walk the (table, pk) prefix; the lex-largest entry is the latest
    // version. We don't filter by any snapshot.
    std::string latest_value;
    bool found = false;
    for (it->Seek(scan_prefix);
         it->Valid() && it->key().starts_with(scan_prefix); it->Next()) {
        latest_value = it->value().ToString();
        found = true;
    }
    if (!found) {
        return std::nullopt;
    }
    auto columns = nlohmann::json::parse(latest_value);
    return columns.get<std::map<std::string, std::string>>();
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
    const std::vector<std::string>& values, int64_t ts) {
    // Build JSON object from columns
    nlohmann::json obj;
    for (int i = 0; i < table->columns().size(); ++i) {
        obj[table->columns()[i].name()] = values[i];
    }

    // Format the caller-supplied ts as a zero-padded 20-digit string so
    // lex order on the key suffix matches chronological order.
    std::ostringstream ts_str;
    ts_str << std::setw(20) << std::setfill('0') << ts;

    auto key = absl::StrFormat("/%s/%s/%s", table->name(), pk, ts_str.str());
    db_->Put(rocksdb::WriteOptions(), key, obj.dump());
}

}  // namespace small::rocks
