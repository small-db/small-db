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

#include <cstdint>
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
    /**
     * @brief Returns the RocksDB singleton for the current server process.
     *
     * Resolves the data directory from the global server_info. Equivalent to
     * GetInstance(server_info::get_info()->db_path).
     */
    static absl::StatusOr<RocksDBWrapper*> GetInstance();

    /**
     * @brief Returns the RocksDB singleton for an explicit path.
     *
     * Per-path singleton: repeated calls with the same db_path return the
     * same instance. Used by tests that open ad-hoc databases outside of a
     * full server process.
     */
    static RocksDBWrapper* GetInstance(const std::string& db_path);

    // copy blocker
    RocksDBWrapper(const RocksDBWrapper&) = delete;

    // assignment blocker
    void operator=(const RocksDBWrapper&) = delete;

    /**
     * @brief Reads the latest visible MVCC version of every row in a table.
     *
     * Scans keys with the prefix "/{table_name}/" and, for each primary
     * key, returns the row version with the largest timestamp suffix
     * that is still <= snapshot_ts. Versions written after snapshot_ts
     * are invisible to this read.
     *
     * @param table_name  Schema-qualified table name (e.g. "system.tables").
     * @param snapshot_ts Snapshot timestamp; only versions with
     *                    version_ts <= snapshot_ts are visible.
     * @return Map of {primary_key -> {column_name -> value}}.
     */
    std::map<std::string, std::map<std::string, std::string>> ReadTable(
        const std::string& table_name, int64_t snapshot_ts);

    /**
     * @brief Hard-deletes a single raw key.
     *
     * Removes the underlying RocksDB entry directly; this is not an MVCC
     * tombstone and does not interact with row-version semantics.
     */
    bool Delete(const std::string& key);

    /**
     * @brief Dumps every key/value pair to stdout. Debug aid only.
     */
    void PrintAllKV();

    /**
     * @brief Writes a new MVCC version of a row at the given timestamp.
     *
     * Encodes the column values as a JSON object and stores them under
     * the key "/{table}/{pk}/{ts}", where ts is the caller-supplied
     * transaction timestamp formatted as a zero-padded 20-digit ms value.
     * Each call appends a new version; old versions are not overwritten.
     *
     * @param table  Table schema; supplies the column order and table name.
     * @param pk     Primary key value, used as the second key segment.
     * @param values Column values in the same order as table->columns().
     * @param ts     Transaction timestamp (ms since epoch). All rows
     *               written by the same transaction must share this value.
     */
    void WriteRow(const std::shared_ptr<small::schema::Table>& table,
                  const std::string& pk,
                  const std::vector<std::string>& values, int64_t ts);

   private:
    rocksdb::DB* db_ = nullptr;

    void Close();
};

}  // namespace small::rocks
