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
#include <optional>
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
     * @brief Reads the most recent committed version of a single row,
     *        ignoring any snapshot filter.
     *
     * Used inside an UPDATE's read-modify-write under the per-row lock,
     * where the writer must see the absolutely-latest committed value
     * regardless of the coordinator's snapshot timestamp. SELECT goes
     * through ReadTable; only the write path uses ReadLatest.
     *
     * @param table_name Schema-qualified table name.
     * @param pk         Primary key.
     * @return The latest version's columns, or nullopt if no version exists.
     */
    std::optional<std::map<std::string, std::string>> ReadLatest(
        const std::string& table_name, const std::string& pk);

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
     * Stores the column values JSON-encoded under the key
     * "/{table}/{pk}/{ts}", where `ts` is formatted as a zero-padded
     * 20-digit value so lex order on the key suffix matches
     * chronological order.
     *
     * @param table  Table schema; supplies column order and table name.
     * @param pk     Primary key value, used as the second key segment.
     * @param values Column values in the same order as table->columns().
     * @param ts     Version timestamp (ms since epoch). Used as-is.
     */
    void WriteRow(const std::shared_ptr<small::schema::Table>& table,
                  const std::string& pk,
                  const std::vector<std::string>& values, int64_t ts);

   private:
    rocksdb::DB* db_ = nullptr;

    void Close();
};

}  // namespace small::rocks
