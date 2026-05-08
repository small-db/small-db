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
#include "rocksdb/db.h"

#include "nlohmann/json.hpp"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/schema/schema.pb.h"

namespace small::rocks {

// Status of a transaction record persisted under /_txn/<txn_id>.
//
// ACTIVE     -- in flight; the writer is still issuing intents.
// COMMITTED  -- finalized; write_ts is the txn's commit timestamp,
//               authoritative for every intent it wrote.
// ABORTED    -- the writer rolled back; readers should skip its intents.
enum class TxnStatus {
    ACTIVE = 0,
    COMMITTED = 1,
    ABORTED = 2,
};

void to_json(nlohmann::json& j, const TxnStatus& s);
void from_json(const nlohmann::json& j, TxnStatus& s);

// On-disk record persisted at /_txn/<txn_id> on the coordinator,
// holding authoritative status for every intent tagged with this
// txn_id. The coordinator is the only writer.
//
// `write_ts` is provisional while ACTIVE, the txn's final commit
// timestamp once COMMITTED, and meaningless while ABORTED.
struct TxnRecord {
    TxnStatus status = TxnStatus::ACTIVE;
    int64_t start_ts = 0;
    int64_t write_ts = 0;

    // Keys of intents this transaction has written (e.g. "/users/3/INTENT").
    // Consumed by a future sweeper / recovery path, not the live
    // commit/rollback protocol.
    std::vector<std::string> intent_keys;
};

void to_json(nlohmann::json& j, const TxnRecord& r);
void from_json(const nlohmann::json& j, TxnRecord& r);

// On-disk intent stored at /<table>/<pk>/INTENT. The literal suffix
// "INTENT" sorts above every 20-digit numeric version_ts so a prefix
// scan over /<table>/<pk>/ surfaces it last.
struct IntentRow {
    std::map<std::string, std::string> values;
    int64_t txn_id = 0;
    std::string coordinator_addr;
};

void to_json(nlohmann::json& j, const IntentRow& r);
void from_json(const nlohmann::json& j, IntentRow& r);

class RocksDBWrapper {
   private:
    explicit RocksDBWrapper(const std::string& db_path);
    ~RocksDBWrapper();

   public:
    // Singleton for the current server process; data dir from server_info.
    static absl::StatusOr<RocksDBWrapper*> GetInstance();

    // Per-path singleton: repeated calls with the same db_path return the
    // same instance. Used by tests that open ad-hoc databases outside of a
    // full server process.
    static RocksDBWrapper* GetInstance(const std::string& db_path);

    RocksDBWrapper(const RocksDBWrapper&) = delete;
    void operator=(const RocksDBWrapper&) = delete;

    // Hard-deletes a single raw key. Not an MVCC tombstone; does not
    // interact with row-version semantics.
    bool Delete(const std::string& key);

    // Writes a new MVCC version of a row at the given timestamp.
    //
    // Stores the column values JSON-encoded under the key
    // "/{table}/{pk}/{ts}", where `ts` is zero-padded to 20 digits so
    // lex order on the key suffix matches chronological order.
    void WriteRow(const std::shared_ptr<small::schema::Table>& table,
                  const std::string& pk, const std::vector<std::string>& values,
                  int64_t ts);

    // Largest committed `version_ts` on (table, pk), or 0 if no committed
    // version exists. Inspects only numeric-suffix keys; an unresolved
    // INTENT key is ignored at this layer.
    int64_t LatestVersionTs(const std::string& table_name,
                            const std::string& pk);

    // Writes an intent at /<table>/<pk>/INTENT, overwriting any existing
    // intent. Caller must hold lock(table, pk).
    void WriteIntent(const std::shared_ptr<small::schema::Table>& table,
                     const std::string& pk,
                     const std::vector<std::string>& values, int64_t txn_id,
                     const std::string& coordinator_addr);

    std::optional<IntentRow> ReadIntent(const std::string& table_name,
                                        const std::string& pk);

    // Overwrites any prior record for the same txn_id. The coordinator is
    // the only writer of its own txn records.
    void WriteTxnRecord(int64_t txn_id, const TxnRecord& record);

    std::optional<TxnRecord> ReadTxnRecord(int64_t txn_id);

    // Read-modify-write: bump write_ts on /_txn/<txn_id>.
    void UpdateTxnWriteTs(int64_t txn_id, int64_t write_ts);

    // Read-modify-write: append `intent_key` to the txn record's
    // intent_keys[].
    void AppendTxnIntentKey(int64_t txn_id, const std::string& intent_key);

    // Flips the txn's status (and, for COMMITTED, records write_ts as
    // the final commit timestamp). One Put — the atomicity boundary
    // for every intent this txn has written.
    void SetTxnStatus(int64_t txn_id, TxnStatus status, int64_t write_ts);

    // Raw view of `/<table>/<pk>/`'s latest state. The caller resolves
    // the intent and applies whatever policy matters for its call site.
    struct LatestRowRaw {
        // Largest committed `version_ts` found by the numeric prefix
        // scan. -1 if no committed version exists for this row.
        int64_t latest_numeric_ts = -1;
        // Column map at latest_numeric_ts. Empty if latest_numeric_ts < 0.
        std::map<std::string, std::string> latest_numeric_value;
        // Current `/<table>/<pk>/INTENT`, if any. Caller resolves
        // separately.
        std::optional<IntentRow> intent;
    };

    // Largest numeric version (value + ts) and the current intent (if
    // any) at `/<table>/<pk>/`, observed in one prefix scan with no RPC.
    LatestRowRaw ReadLatestRaw(const std::string& table_name,
                               const std::string& pk);

    // Callback that resolves an intent into (is_committed, write_ts).
    // The returned write_ts is meaningful only when is_committed is true;
    // it is the resolved txn's finalized write_ts (post-COMMIT).
    using IntentResolver =
        std::function<absl::StatusOr<std::pair<bool, int64_t>>(
            const IntentRow&)>;

    // Writer-side intent promotion: persist the value as a numeric MVCC
    // version AND delete the intent slot in one atomic write batch.
    // Caller MUST hold lock(table, pk) — the Delete is path-addressed
    // and would race with a concurrent slot mutation otherwise.
    void PromoteIntent(const std::string& table_name, const std::string& pk,
                       int64_t write_ts,
                       const std::map<std::string, std::string>& values);

    // Latest visible MVCC version of every row in the table at
    // snapshot_ts. For each pk, picks the larger of (numeric
    // version_ts <= snapshot_ts) and (resolved write_ts of a COMMITTED
    // INTENT whose write_ts <= snapshot_ts).
    std::map<std::string, std::map<std::string, std::string>>
    ReadTableWithResolver(const std::string& table_name, int64_t snapshot_ts,
                          const IntentResolver& resolver);

    // Latest committed version of a single row, surfacing a COMMITTED
    // intent whose write_ts beats the largest numeric version_ts.
    // Returns nullopt if no version exists.
    std::optional<std::map<std::string, std::string>> ReadLatestWithResolver(
        const std::string& table_name, const std::string& pk,
        const IntentResolver& resolver);

   private:
    rocksdb::DB* db_ = nullptr;

    void Close();
};

}  // namespace small::rocks
