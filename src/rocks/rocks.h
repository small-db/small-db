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
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

#include "absl/status/statusor.h"
#include "nlohmann/json.hpp"
#include "rocksdb/db.h"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/schema/schema.pb.h"

namespace small::rocks {

// Status of a transaction record persisted under /_txn/<txn_id>.
//
// ACTIVE     -- in flight; the writer is still issuing intents.
// COMMITTED  -- finalized; commit_ts is authoritative for every intent
//               this transaction wrote.
// ABORTED    -- the writer rolled back; readers should skip its intents.
enum class TxnStatus {
    ACTIVE = 0,
    COMMITTED = 1,
    ABORTED = 2,
};

void to_json(nlohmann::json& j, const TxnStatus& s);
void from_json(const nlohmann::json& j, TxnStatus& s);

// On-disk record persisted at /_txn/<txn_id> on the coordinator. Holds
// the authoritative status for every intent tagged with this txn_id.
//
// The coordinator is the only writer. Readers on any node consult it
// via the TxnService gRPC when resolving an intent.
struct TxnRecord {
    TxnStatus status = TxnStatus::ACTIVE;
    int64_t start_ts = 0;
    int64_t commit_ts = 0;

    // Keys of intents this transaction has written (e.g. "/users/3/INTENT").
    // Populated as intents are dispatched. Consumed by a future sweeper /
    // recovery path, not by the live commit/rollback protocol.
    std::vector<std::string> intent_keys;
};

void to_json(nlohmann::json& j, const TxnRecord& r);
void from_json(const nlohmann::json& j, TxnRecord& r);

// On-disk intent stored at /<table>/<pk>/INTENT. The literal suffix
// "INTENT" sorts above every 20-digit numeric version_ts so a prefix
// scan over /<table>/<pk>/ surfaces it last.
//
// `values` is the column map (column_name -> encoded value), same shape
// as a committed row. `txn_id` keys the coordinator's /_txn/<txn_id>
// record; `coordinator_addr` is the gRPC endpoint to RPC for resolution.
struct IntentRow {
    std::map<std::string, std::string> values;
    int64_t txn_id = 0;
    std::string coordinator_addr;
};

void to_json(nlohmann::json& j, const IntentRow& r);
void from_json(const nlohmann::json& j, IntentRow& r);

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

    /**
     * @brief Returns the largest committed `version_ts` on (table, pk),
     *        or 0 if no committed version exists.
     *
     * Inspects only numeric-suffix keys; an unresolved INTENT key is
     * ignored at this layer. The intent-aware variant -- which RPC's the
     * coordinator and treats COMMITTED intents as candidates -- is layered
     * on top in src/txn/.
     */
    int64_t LatestVersionTs(const std::string& table_name,
                            const std::string& pk);

    /**
     * @brief Writes an intent at /<table>/<pk>/INTENT.
     *
     * Overwrites any existing intent for the same (table, pk). The caller
     * is expected to hold lock(table, pk) so this is the only writer.
     */
    void WriteIntent(const std::shared_ptr<small::schema::Table>& table,
                     const std::string& pk,
                     const std::vector<std::string>& values, int64_t txn_id,
                     const std::string& coordinator_addr);

    /**
     * @brief Reads the intent at /<table>/<pk>/INTENT, if any.
     */
    std::optional<IntentRow> ReadIntent(const std::string& table_name,
                                        const std::string& pk);

    /**
     * @brief Hard-deletes the intent at /<table>/<pk>/INTENT, if any.
     */
    bool DeleteIntent(const std::string& table_name, const std::string& pk);

    /**
     * @brief Writes the txn record at /_txn/<txn_id>.
     *
     * Overwrites any prior record for the same txn_id. The coordinator is
     * the only writer of its own txn records.
     */
    void WriteTxnRecord(int64_t txn_id, const TxnRecord& record);

    /**
     * @brief Reads the txn record at /_txn/<txn_id>, if any.
     */
    std::optional<TxnRecord> ReadTxnRecord(int64_t txn_id);

    /**
     * @brief Read-modify-write: bump commit_ts on /_txn/<txn_id>.
     *
     * Used by the push protocol when a writer encounters a row whose
     * latest committed version is >= the current commit_ts.
     */
    void UpdateTxnCommitTs(int64_t txn_id, int64_t commit_ts);

    /**
     * @brief Read-modify-write: append `intent_key` to the txn record's
     *        intent_keys[].
     */
    void AppendTxnIntentKey(int64_t txn_id, const std::string& intent_key);

    /**
     * @brief Read-modify-write: flip the txn's status (and, for COMMITTED,
     *        finalize its commit_ts). One Put -- the atomicity boundary
     *        for every intent this txn has written.
     */
    void SetTxnStatus(int64_t txn_id, TxnStatus status, int64_t commit_ts);

    /**
     * @brief Callback that resolves an intent into (is_committed, commit_ts).
     *
     * Used by the With-Resolver read variants to delegate intent
     * resolution out of this network-free layer. The wrapper supplied
     * by src/txn/ implements the gRPC RPC under the hood.
     */
    using IntentResolver =
        std::function<absl::StatusOr<std::pair<bool, int64_t>>(
            const IntentRow&)>;

    /**
     * @brief Reader-side intent promotion: persist a resolved
     *        COMMITTED intent's value as a numeric MVCC version.
     *
     * Writes only `/<table>/<pk>/<commit_ts>` -- the intent slot is
     * left in place. Lock-free and idempotent: any number of readers
     * may race this Put against each other or against a writer's
     * `FullPromoteIntent` and the result is identical (same key, same
     * value derived from the txn's permanent commit_ts).
     */
    void HalfPromoteIntent(const std::string& table_name,
                           const std::string& pk, int64_t commit_ts,
                           const std::map<std::string, std::string>& values);

    /**
     * @brief Writer-side intent promotion: persist the value as a
     *        numeric MVCC version AND delete the intent slot in one
     *        atomic write batch.
     *
     * Caller MUST hold `lock(table, pk)`. The Delete is path-addressed
     * (`/<table>/<pk>/INTENT`) and would race with a concurrent slot
     * mutation if no lock were held.
     */
    void FullPromoteIntent(const std::string& table_name,
                           const std::string& pk, int64_t commit_ts,
                           const std::map<std::string, std::string>& values);

    /**
     * @brief Variant of ReadTable that surfaces COMMITTED intents whose
     *        commit_ts is <= snapshot_ts.
     *
     * For each pk under the table prefix, picks the lex-largest visible
     * source of truth: the largest numeric `version_ts <= snapshot_ts`,
     * OR the resolved `commit_ts` of an INTENT (if its txn is COMMITTED
     * and commit_ts <= snapshot_ts), whichever is larger.
     */
    std::map<std::string, std::map<std::string, std::string>>
    ReadTableWithResolver(const std::string& table_name, int64_t snapshot_ts,
                          const IntentResolver& resolver);

    /**
     * @brief Variant of ReadLatest that surfaces a COMMITTED intent if
     *        its commit_ts is greater than the largest numeric
     *        version_ts on the row.
     *
     * Used by writers in their pre-image read: a COMMITTED intent left
     * by a prior transaction (not yet promoted) is the row's "current"
     * state, and the next writer must compute on top of it.
     */
    std::optional<std::map<std::string, std::string>> ReadLatestWithResolver(
        const std::string& table_name, const std::string& pk,
        const IntentResolver& resolver);

   private:
    rocksdb::DB* db_ = nullptr;

    void Close();
};

}  // namespace small::rocks
