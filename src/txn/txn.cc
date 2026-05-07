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
#include <map>
#include <optional>
#include <string>
#include <utility>

// =====================================================================
// third-party libraries
// =====================================================================

#include "absl/strings/str_format.h"
#include "grpcpp/create_channel.h"
#include "spdlog/spdlog.h"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/rocks/rocks.h"

// =====================================================================
// self header
// =====================================================================

#include "src/txn/txn.h"

namespace small::txn {

grpc::Status TxnServiceImpl::ResolveIntent(
    grpc::ServerContext* context, const ResolveIntentRequest* request,
    ResolveIntentResponse* response) {
    auto db_or = small::rocks::RocksDBWrapper::GetInstance();
    if (!db_or.ok()) {
        return {grpc::StatusCode::INTERNAL,
                std::string(db_or.status().message())};
    }
    auto db = db_or.value();
    auto record = db->ReadTxnRecord(request->txn_id());
    if (!record.has_value()) {
        // No record on this server -- intent is orphaned, or the caller
        // RPC'd the wrong coordinator. Either way, ABORTED-equivalent.
        response->set_status(ResolveIntentResponse::UNKNOWN);
        response->set_commit_ts(0);
        return grpc::Status::OK;
    }
    switch (record->status) {
        case small::rocks::TxnStatus::ACTIVE:
            response->set_status(ResolveIntentResponse::ACTIVE);
            break;
        case small::rocks::TxnStatus::COMMITTED:
            response->set_status(ResolveIntentResponse::COMMITTED);
            // record->write_ts is the txn's final commit timestamp now
            // that status is COMMITTED.
            response->set_commit_ts(record->write_ts);
            break;
        case small::rocks::TxnStatus::ABORTED:
            response->set_status(ResolveIntentResponse::ABORTED);
            break;
    }
    return grpc::Status::OK;
}

absl::StatusOr<ResolveIntentResponse> resolve_intent(
    const std::string& coordinator_addr, int64_t txn_id) {
    auto channel = grpc::CreateChannel(coordinator_addr,
                                       grpc::InsecureChannelCredentials());
    auto stub = TxnService::NewStub(channel);
    grpc::ClientContext context;
    ResolveIntentRequest request;
    request.set_txn_id(txn_id);
    ResolveIntentResponse response;
    auto status = stub->ResolveIntent(&context, request, &response);
    if (!status.ok()) {
        return absl::InternalError(absl::StrFormat(
            "ResolveIntent rpc to %s for txn_id=%d failed: %s",
            coordinator_addr, txn_id, status.error_message()));
    }
    return response;
}

// gRPC-backed resolver, supplied to RocksDBWrapper's With-Resolver
// methods so the rocks layer stays network-free.
static small::rocks::RocksDBWrapper::IntentResolver default_resolver() {
    return [](const small::rocks::IntentRow& intent)
        -> absl::StatusOr<std::pair<bool, int64_t>> {
        auto resp = resolve_intent(intent.coordinator_addr, intent.txn_id);
        if (!resp.ok()) return resp.status();
        if (resp->status() == ResolveIntentResponse::COMMITTED) {
            return std::make_pair(true, resp->commit_ts());
        }
        return std::make_pair(false, int64_t{0});
    };
}

absl::StatusOr<std::optional<WriterPreimage>> read_for_writer(
    const std::string& table_name, const std::string& pk) {
    auto db_or = small::rocks::RocksDBWrapper::GetInstance();
    if (!db_or.ok()) return db_or.status();
    auto db = db_or.value();

    auto raw = db->ReadLatestRaw(table_name, pk);

    // No intent on this row: numeric state is the truth.
    if (!raw.intent.has_value()) {
        if (raw.latest_numeric_ts < 0) {
            // Row doesn't exist on this node -- caller is a non-owner.
            return std::optional<WriterPreimage>{};
        }
        return std::optional<WriterPreimage>{WriterPreimage{
            std::move(raw.latest_numeric_value),
            raw.latest_numeric_ts,
        }};
    }

    auto resp_or =
        resolve_intent(raw.intent->coordinator_addr, raw.intent->txn_id);
    if (!resp_or.ok()) return resp_or.status();
    const auto& resp = resp_or.value();

    switch (resp.status()) {
        case ResolveIntentResponse::COMMITTED: {
            // Caller holds lock(table, pk) -- safe to do the full
            // promotion (numeric Put + intent Delete). The resolved
            // intent's commit_ts and value are the row's true latest
            // when commit_ts >= latest_numeric_ts, which is the
            // typical case under our protocol; fall through to the
            // numeric path otherwise.
            int64_t commit_ts = resp.commit_ts();
            db->PromoteIntent(table_name, pk, commit_ts, raw.intent->values);
            if (commit_ts >= raw.latest_numeric_ts) {
                return std::optional<WriterPreimage>{WriterPreimage{
                    raw.intent->values,
                    commit_ts,
                }};
            }
            return std::optional<WriterPreimage>{WriterPreimage{
                std::move(raw.latest_numeric_value),
                raw.latest_numeric_ts,
            }};
        }
        case ResolveIntentResponse::ABORTED:
        case ResolveIntentResponse::UNKNOWN:
            // Intent is dead; the caller's WriteIntent will overwrite
            // the slot. Numeric state is the truth.
            if (raw.latest_numeric_ts < 0) {
                return std::optional<WriterPreimage>{};
            }
            return std::optional<WriterPreimage>{WriterPreimage{
                std::move(raw.latest_numeric_value),
                raw.latest_numeric_ts,
            }};
        case ResolveIntentResponse::ACTIVE:
            // The intent's coordinator is still in flight. The caller
            // must abort and retry; pushing the other transaction or
            // queueing a waiter is deferred to a later page.
            return absl::AbortedError(absl::StrFormat(
                "active intent on %s/%s for txn_id=%d; retry",
                table_name, pk, raw.intent->txn_id));
        default:
            SPDLOG_ERROR(
                "read_for_writer: unknown ResolveIntent status {} for "
                "txn_id={}",
                static_cast<int>(resp.status()), raw.intent->txn_id);
            // Treat as not-committed.
            if (raw.latest_numeric_ts < 0) {
                return std::optional<WriterPreimage>{};
            }
            return std::optional<WriterPreimage>{WriterPreimage{
                std::move(raw.latest_numeric_value),
                raw.latest_numeric_ts,
            }};
    }
}

std::map<std::string, std::map<std::string, std::string>>
read_table_at_snapshot(const std::string& table_name, int64_t snapshot_ts) {
    auto db = small::rocks::RocksDBWrapper::GetInstance().value();
    return db->ReadTableWithResolver(table_name, snapshot_ts,
                                     default_resolver());
}

}  // namespace small::txn
