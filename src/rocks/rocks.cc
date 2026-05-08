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
#include <map>
#include <memory>
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
#include "rocksdb/write_batch.h"

// absl
#include "absl/status/statusor.h"

// spdlog
#include "spdlog/spdlog.h"

// nlohmann/json
#include "nlohmann/json.hpp"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/server_info/info.h"
#include "src/util/time/time.h"

// =====================================================================
// self header
// =====================================================================

#include "src/rocks/rocks.h"

#include "src/rocks/keys.h"

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
        {"write_ts", r.write_ts},
        {"intent_keys", r.intent_keys},
    };
}

void from_json(const nlohmann::json& j, TxnRecord& r) {
    j.at("status").get_to(r.status);
    j.at("start_ts").get_to(r.start_ts);
    j.at("write_ts").get_to(r.write_ts);
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
    instances[db_path] = new RocksDBWrapper(db_path);
    return instances[db_path];
}

RocksDBWrapper::RocksDBWrapper(const std::string& db_path) {
    bool _ = std::filesystem::create_directories(db_path);

    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db_);
    if (!status.ok()) {
        throw std::runtime_error("Failed to open RocksDB: " +
                                 status.ToString());
    }
}

RocksDBWrapper::~RocksDBWrapper() { Close(); }

void RocksDBWrapper::Close() { delete db_; }

bool RocksDBWrapper::Delete(const std::string& key) {
    rocksdb::Status status = db_->Delete(rocksdb::WriteOptions(), key);
    return status.ok();
}

void RocksDBWrapper::WriteRow(
    const std::shared_ptr<small::schema::Table>& table, const std::string& pk,
    const std::vector<std::string>& values, int64_t ts) {
    nlohmann::json obj;
    for (int i = 0; i < table->columns().size(); ++i) {
        obj[table->columns()[i].name()] = values[i];
    }

    auto key = VersionKey(table->name(), pk, ts);
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

int64_t RocksDBWrapper::LatestVersionTs(const std::string& table_name,
                                        const std::string& pk) {
    auto scan_prefix = RowPrefix(table_name, pk);

    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options));

    int64_t latest = 0;
    for (it->Seek(scan_prefix);
         it->Valid() && it->key().starts_with(scan_prefix); it->Next()) {
        std::string key = it->key().ToString();
        std::string suffix = key.substr(scan_prefix.length());
        if (suffix == kIntentSuffix) continue;
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

    auto key = IntentKey(table->name(), pk);
    nlohmann::json j = intent;
    SPDLOG_INFO("WriteIntent: key={} txn_id={} coordinator={}", key, txn_id,
                coordinator_addr);
    db_->Put(rocksdb::WriteOptions(), key, j.dump());
}

std::optional<IntentRow> RocksDBWrapper::ReadIntent(
    const std::string& table_name, const std::string& pk) {
    auto key = IntentKey(table_name, pk);
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

void RocksDBWrapper::WriteTxnRecord(int64_t txn_id, const TxnRecord& record) {
    auto key = TxnKey(txn_id);
    nlohmann::json j = record;
    SPDLOG_INFO("WriteTxnRecord: key={} status={} write_ts={} ({})", key,
                static_cast<int>(record.status), record.write_ts,
                small::util::FormatTsMs(record.write_ts));
    db_->Put(rocksdb::WriteOptions(), key, j.dump());
}

std::optional<TxnRecord> RocksDBWrapper::ReadTxnRecord(int64_t txn_id) {
    auto key = TxnKey(txn_id);
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

void RocksDBWrapper::UpdateTxnWriteTs(int64_t txn_id, int64_t write_ts) {
    auto record = ReadTxnRecord(txn_id);
    if (!record.has_value()) {
        SPDLOG_ERROR("UpdateTxnWriteTs: no record for txn_id={}", txn_id);
        return;
    }
    record->write_ts = write_ts;
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
                                  int64_t write_ts) {
    auto record = ReadTxnRecord(txn_id);
    if (!record.has_value()) {
        SPDLOG_ERROR("SetTxnStatus: no record for txn_id={}", txn_id);
        return;
    }
    record->status = status;
    if (status == TxnStatus::COMMITTED) record->write_ts = write_ts;
    WriteTxnRecord(txn_id, record.value());
}

RocksDBWrapper::LatestRowRaw RocksDBWrapper::ReadLatestRaw(
    const std::string& table_name, const std::string& pk) {
    auto scan_prefix = "/" + table_name + "/" + pk + "/";

    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options));

    LatestRowRaw out;
    for (it->Seek(scan_prefix);
         it->Valid() && it->key().starts_with(scan_prefix); it->Next()) {
        std::string suffix =
            it->key().ToString().substr(scan_prefix.length());
        std::string raw = it->value().ToString();

        if (suffix == kIntentSuffix) {
            out.intent = nlohmann::json::parse(raw).get<IntentRow>();
            continue;
        }

        int64_t version_ts;
        try {
            version_ts = std::stoll(suffix);
        } catch (const std::exception&) {
            SPDLOG_WARN("ReadLatestRaw: skipping unparseable suffix {}",
                        suffix);
            continue;
        }
        if (version_ts > out.latest_numeric_ts) {
            out.latest_numeric_ts = version_ts;
            out.latest_numeric_value =
                nlohmann::json::parse(raw)
                    .get<std::map<std::string, std::string>>();
        }
    }
    return out;
}

std::map<std::string, std::map<std::string, std::string>>
RocksDBWrapper::ReadTableWithResolver(const std::string& table_name,
                                      int64_t snapshot_ts,
                                      const IntentResolver& resolver) {
    auto scan_prefix = TablePrefix(table_name);

    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;

    std::map<std::string,
             std::pair<int64_t, std::map<std::string, std::string>>>
        best;

    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options));
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
            int64_t write_ts = pair.second;
            if (write_ts > snapshot_ts) continue;
            auto& entry = best[pk];
            if (write_ts > entry.first) {
                entry = {write_ts, intent.values};
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
    auto scan_prefix = RowPrefix(table_name, pk);

    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;

    int64_t best_ts = -1;
    std::map<std::string, std::string> best_value;

    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options));
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
            int64_t write_ts = pair.second;
            if (write_ts > best_ts) {
                best_ts = write_ts;
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

void RocksDBWrapper::PromoteIntent(
    const std::string& table_name, const std::string& pk, int64_t write_ts,
    const std::map<std::string, std::string>& values) {
    rocksdb::WriteBatch batch;
    nlohmann::json obj = values;
    batch.Put(VersionKey(table_name, pk, write_ts), obj.dump());
    batch.Delete(IntentKey(table_name, pk));
    auto status = db_->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok()) {
        SPDLOG_ERROR("PromoteIntent({}/{}): {}", table_name, pk,
                     status.ToString());
    }
}

}  // namespace small::rocks
