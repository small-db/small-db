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
     * @brief Retrieves all rows from a table at a snapshot.
     *
     * @param table_name Name of the table to read
     * @param snapshot_ts_millis Snapshot timestamp in milliseconds. 0 means
     *        "no filter" — return the latest version per pk regardless of ts
     *        (equivalent to a "read latest" query). Non-zero filters out
     *        versions written after the snapshot, so the result is the
     *        latest version per pk with version_ts <= snapshot_ts.
     * @return Map structure: {primary_key -> {column_name -> value}}
     */
    std::map<std::string, std::map<std::string, std::string>> ReadTable(
        const std::string& table_name, int64_t snapshot_ts_millis = 0);

    std::vector<std::pair<std::string, std::string>> GetAllKV();

    bool Delete(const std::string& key);

    void PrintAllKV();

    /**
     * @brief Writes a row version.
     *
     * @param commit_ts_millis Commit timestamp in milliseconds. 0 means
     *        "use now()" — used for auto-commit writes outside of any
     *        explicit transaction. Non-zero stamps the version with the
     *        given timestamp so all writes from a single transaction can
     *        share one commit_ts and become visible atomically.
     */
    void WriteRow(const std::shared_ptr<small::schema::Table>& table,
                  const std::string& pk,
                  const std::vector<std::string>& values,
                  int64_t commit_ts_millis = 0);

   private:
    rocksdb::DB* db_ = nullptr;

    void Close();
};

}  // namespace small::rocks
