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
#include <utility>
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

// spdlog
#include "spdlog/spdlog.h"

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

// ---------------------------------------------------------------------
// JSON serialization for TxnStatus, TxnRecord, IntentRow.
// ---------------------------------------------------------------------

void to_json(nlohmann::json& j, const TxnStatus& s) {
    switch (s) {
        case TxnStatus::ACTIVE:
            j = "ACTIVE";
            break;
        case TxnStatus::COMMITTED:
            j = "COMMITTED";
            break;
        case TxnStatus::ABORTED:
            j = "ABORTED";
            break;
    }
}

void from_json(const nlohmann::json& j, TxnStatus& s) {
    auto str = j.get<std::string>();
    if (str == "ACTIVE") {
        s = TxnStatus::ACTIVE;
    } else if (str == "COMMITTED") {
        s = TxnStatus::COMMITTED;
    } else if (str == "ABORTED") {
        s = TxnStatus::ABORTED;
    } else {
        throw std::runtime_error("unknown TxnStatus: " + str);
    }
}

void to_json(nlohmann::json& j, const TxnRecord& r) {
    j = nlohmann::json{
        {"status", r.status},
        {"start_ts", r.start_ts},
        {"commit_ts", r.commit_ts},
        {"intent_keys", r.intent_keys},
    };
}

void from_json(const nlohmann::json& j, TxnRecord& r) {
    j.at("status").get_to(r.status);
    j.at("start_ts").get_to(r.start_ts);
    j.at("commit_ts").get_to(r.commit_ts);
    j.at("intent_keys").get_to(r.intent_keys);
}

void to_json(nlohmann::json& j, const IntentRow& r) {
    j = nlohmann::json{
        {"values", r.values},
        {"txn_id", r.txn_id},
        {"coordinator_addr", r.coordinator_addr},
    };
}

void from_json(const nlohmann::json& j, IntentRow& r) {
    j.at("values").get_to(r.values);
    j.at("txn_id").get_to(r.txn_id);
    j.at("coordinator_addr").get_to(r.coordinator_addr);
}

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

        // Third segment is either a 20-digit zero-padded ms timestamp
        // or the literal "INTENT" suffix. Skip intents at this layer;
        // the intent-aware variant lives in src/txn/.
        std::string ts_str = key.substr(pk_end + 1);
        if (ts_str == "INTENT") continue;
        int64_t version_ts;
        try {
            version_ts = std::stoll(ts_str);
        } catch (const std::exception&) {
            SPDLOG_WARN("ReadTable: skipping unparseable suffix {}", ts_str);
            continue;
        }

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

    // Walk the (table, pk) prefix and remember the latest numeric
    // version_ts and its value. INTENT keys are skipped here; the
    // intent-aware variant lives in src/txn/.
    int64_t latest_ts = -1;
    std::string latest_value;
    for (it->Seek(scan_prefix);
         it->Valid() && it->key().starts_with(scan_prefix); it->Next()) {
        std::string suffix =
            it->key().ToString().substr(scan_prefix.length());
        if (suffix == "INTENT") continue;
        int64_t version_ts;
        try {
            version_ts = std::stoll(suffix);
        } catch (const std::exception&) {
            SPDLOG_WARN("ReadLatest: skipping unparseable suffix {}", suffix);
            continue;
        }
        if (version_ts > latest_ts) {
            latest_ts = version_ts;
            latest_value = it->value().ToString();
        }
    }
    if (latest_ts < 0) {
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
    SPDLOG_INFO("WriteRow: key={}", key);
    db_->Put(rocksdb::WriteOptions(), key, obj.dump());
}

// ---------------------------------------------------------------------
// Intents and transaction records.
// ---------------------------------------------------------------------

// All numeric version_ts values are 20-digit zero-padded strings, which
// lex-sort below the literal "INTENT" suffix. So a prefix scan over
// /<table>/<pk>/ surfaces committed versions first and the intent (if
// any) last.
static constexpr const char* kIntentSuffix = "INTENT";

static std::string intent_key(const std::string& table_name,
                              const std::string& pk) {
    return absl::StrFormat("/%s/%s/%s", table_name, pk, kIntentSuffix);
}

static std::string txn_key(int64_t txn_id) {
    return absl::StrFormat("/_txn/%d", txn_id);
}

int64_t RocksDBWrapper::LatestVersionTs(const std::string& table_name,
                                        const std::string& pk) {
    auto scan_prefix = "/" + table_name + "/" + pk + "/";

    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options));

    // Walk numeric version_ts entries; ignore the INTENT key (resolution
    // for that lives in src/txn/).
    int64_t latest = 0;
    for (it->Seek(scan_prefix);
         it->Valid() && it->key().starts_with(scan_prefix); it->Next()) {
        std::string key = it->key().ToString();
        std::string suffix = key.substr(scan_prefix.length());
        if (suffix == kIntentSuffix) continue;
        // Numeric suffix.
        try {
            int64_t ts = std::stoll(suffix);
            if (ts > latest) latest = ts;
        } catch (const std::exception&) {
            SPDLOG_WARN("LatestVersionTs: skipping unparseable suffix {}",
                        suffix);
        }
    }
    return latest;
}

void RocksDBWrapper::WriteIntent(
    const std::shared_ptr<small::schema::Table>& table, const std::string& pk,
    const std::vector<std::string>& values, int64_t txn_id,
    const std::string& coordinator_addr) {
    IntentRow intent;
    for (int i = 0; i < table->columns().size(); ++i) {
        intent.values[table->columns()[i].name()] = values[i];
    }
    intent.txn_id = txn_id;
    intent.coordinator_addr = coordinator_addr;

    auto key = intent_key(table->name(), pk);
    nlohmann::json j = intent;
    SPDLOG_INFO("WriteIntent: key={} txn_id={} coordinator={}", key, txn_id,
                coordinator_addr);
    db_->Put(rocksdb::WriteOptions(), key, j.dump());
}

std::optional<IntentRow> RocksDBWrapper::ReadIntent(
    const std::string& table_name, const std::string& pk) {
    auto key = intent_key(table_name, pk);
    std::string value;
    auto status = db_->Get(rocksdb::ReadOptions(), key, &value);
    if (status.IsNotFound()) return std::nullopt;
    if (!status.ok()) {
        SPDLOG_ERROR("ReadIntent({}): {}", key, status.ToString());
        return std::nullopt;
    }
    auto j = nlohmann::json::parse(value);
    return j.get<IntentRow>();
}

bool RocksDBWrapper::DeleteIntent(const std::string& table_name,
                                  const std::string& pk) {
    auto key = intent_key(table_name, pk);
    auto status = db_->Delete(rocksdb::WriteOptions(), key);
    return status.ok();
}

void RocksDBWrapper::WriteTxnRecord(int64_t txn_id, const TxnRecord& record) {
    auto key = txn_key(txn_id);
    nlohmann::json j = record;
    SPDLOG_INFO("WriteTxnRecord: key={} status={} commit_ts={}", key,
                static_cast<int>(record.status), record.commit_ts);
    db_->Put(rocksdb::WriteOptions(), key, j.dump());
}

std::optional<TxnRecord> RocksDBWrapper::ReadTxnRecord(int64_t txn_id) {
    auto key = txn_key(txn_id);
    std::string value;
    auto status = db_->Get(rocksdb::ReadOptions(), key, &value);
    if (status.IsNotFound()) return std::nullopt;
    if (!status.ok()) {
        SPDLOG_ERROR("ReadTxnRecord({}): {}", key, status.ToString());
        return std::nullopt;
    }
    auto j = nlohmann::json::parse(value);
    return j.get<TxnRecord>();
}

void RocksDBWrapper::UpdateTxnCommitTs(int64_t txn_id, int64_t commit_ts) {
    auto record = ReadTxnRecord(txn_id);
    if (!record.has_value()) {
        SPDLOG_ERROR("UpdateTxnCommitTs: no record for txn_id={}", txn_id);
        return;
    }
    record->commit_ts = commit_ts;
    WriteTxnRecord(txn_id, record.value());
}

void RocksDBWrapper::AppendTxnIntentKey(int64_t txn_id,
                                        const std::string& intent_key) {
    auto record = ReadTxnRecord(txn_id);
    if (!record.has_value()) {
        SPDLOG_ERROR("AppendTxnIntentKey: no record for txn_id={}", txn_id);
        return;
    }
    record->intent_keys.push_back(intent_key);
    WriteTxnRecord(txn_id, record.value());
}

void RocksDBWrapper::SetTxnStatus(int64_t txn_id, TxnStatus status,
                                  int64_t commit_ts) {
    auto record = ReadTxnRecord(txn_id);
    if (!record.has_value()) {
        SPDLOG_ERROR("SetTxnStatus: no record for txn_id={}", txn_id);
        return;
    }
    record->status = status;
    if (status == TxnStatus::COMMITTED) record->commit_ts = commit_ts;
    WriteTxnRecord(txn_id, record.value());
}

std::map<std::string, std::map<std::string, std::string>>
RocksDBWrapper::ReadTableWithResolver(const std::string& table_name,
                                      int64_t snapshot_ts,
                                      const IntentResolver& resolver) {
    auto scan_prefix = "/" + table_name + "/";

    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options));

    // Per pk, remember the largest visible version_ts we've seen and
    // the columns it points to. A pk's slot only advances when a
    // new candidate beats `entry.first`.
    std::map<std::string,
             std::pair<int64_t, std::map<std::string, std::string>>>
        best;

    for (it->Seek(scan_prefix);
         it->Valid() && it->key().starts_with(scan_prefix); it->Next()) {
        std::string key = it->key().ToString();
        std::string value = it->value().ToString();

        size_t start_pos = scan_prefix.length();
        size_t pk_end = key.find('/', start_pos);
        if (pk_end == std::string::npos) continue;
        std::string pk = key.substr(start_pos, pk_end - start_pos);
        std::string suffix = key.substr(pk_end + 1);

        if (suffix == kIntentSuffix) {
            auto intent = nlohmann::json::parse(value).get<IntentRow>();
            auto resolved = resolver(intent);
            if (!resolved.ok()) continue;
            auto pair = resolved.value();
            if (!pair.first) continue;
            int64_t commit_ts = pair.second;
            if (commit_ts > snapshot_ts) continue;
            auto& entry = best[pk];
            if (commit_ts > entry.first) {
                entry = {commit_ts, intent.values};
            }
            continue;
        }

        int64_t version_ts;
        try {
            version_ts = std::stoll(suffix);
        } catch (const std::exception&) {
            SPDLOG_WARN("ReadTableWithResolver: skipping unparseable {}",
                        suffix);
            continue;
        }
        if (version_ts > snapshot_ts) continue;
        auto& entry = best[pk];
        if (version_ts > entry.first) {
            auto cols = nlohmann::json::parse(value)
                            .get<std::map<std::string, std::string>>();
            entry = {version_ts, std::move(cols)};
        }
    }

    std::map<std::string, std::map<std::string, std::string>> result;
    for (auto& [pk, pair] : best) {
        result[pk] = std::move(pair.second);
    }
    return result;
}

std::optional<std::map<std::string, std::string>>
RocksDBWrapper::ReadLatestWithResolver(const std::string& table_name,
                                       const std::string& pk,
                                       const IntentResolver& resolver) {
    auto scan_prefix = "/" + table_name + "/" + pk + "/";

    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options));

    int64_t best_ts = -1;
    std::map<std::string, std::string> best_value;

    for (it->Seek(scan_prefix);
         it->Valid() && it->key().starts_with(scan_prefix); it->Next()) {
        std::string suffix =
            it->key().ToString().substr(scan_prefix.length());
        std::string raw = it->value().ToString();

        if (suffix == kIntentSuffix) {
            auto intent = nlohmann::json::parse(raw).get<IntentRow>();
            auto resolved = resolver(intent);
            if (!resolved.ok()) continue;
            auto pair = resolved.value();
            if (!pair.first) continue;
            int64_t commit_ts = pair.second;
            if (commit_ts > best_ts) {
                best_ts = commit_ts;
                best_value = intent.values;
            }
            continue;
        }

        int64_t version_ts;
        try {
            version_ts = std::stoll(suffix);
        } catch (const std::exception&) {
            SPDLOG_WARN("ReadLatestWithResolver: skipping unparseable {}",
                        suffix);
            continue;
        }
        if (version_ts > best_ts) {
            best_ts = version_ts;
            best_value = nlohmann::json::parse(raw)
                             .get<std::map<std::string, std::string>>();
        }
    }

    if (best_ts < 0) return std::nullopt;
    return best_value;
}

}  // namespace small::rocks
