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
            response->set_commit_ts(record->commit_ts);
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

absl::StatusOr<int64_t> latest_committed_version_ts(
    const std::string& table_name, const std::string& pk) {
    auto db_or = small::rocks::RocksDBWrapper::GetInstance();
    if (!db_or.ok()) return db_or.status();
    auto db = db_or.value();

    int64_t latest = db->LatestVersionTs(table_name, pk);

    auto intent = db->ReadIntent(table_name, pk);
    if (!intent.has_value()) return latest;

    auto resp_or = resolve_intent(intent->coordinator_addr, intent->txn_id);
    if (!resp_or.ok()) return resp_or.status();
    const auto& resp = resp_or.value();

    switch (resp.status()) {
        case ResolveIntentResponse::COMMITTED:
            if (resp.commit_ts() > latest) latest = resp.commit_ts();
            break;
        case ResolveIntentResponse::ABORTED:
        case ResolveIntentResponse::UNKNOWN:
            // Intent is dead; skip.
            break;
        case ResolveIntentResponse::ACTIVE:
            // Should not happen: the caller holds lock(table, pk), and
            // the LIST-partitioning model has exactly one writer per row
            // across the cluster. A pre-existing intent on this row
            // must belong to a transaction that has already finished.
            SPDLOG_WARN(
                "latest_committed_version_ts: unexpected ACTIVE intent "
                "txn_id={} on {}/{}",
                intent->txn_id, table_name, pk);
            break;
        default:
            SPDLOG_ERROR(
                "latest_committed_version_ts: unknown ResolveIntent "
                "status {} for txn_id={}",
                static_cast<int>(resp.status()), intent->txn_id);
            break;
    }
    return latest;
}

std::map<std::string, std::map<std::string, std::string>>
read_table_at_snapshot(const std::string& table_name, int64_t snapshot_ts) {
    auto db = small::rocks::RocksDBWrapper::GetInstance().value();
    return db->ReadTableWithResolver(table_name, snapshot_ts,
                                     default_resolver());
}

std::optional<std::map<std::string, std::string>> read_latest_with_intents(
    const std::string& table_name, const std::string& pk) {
    auto db = small::rocks::RocksDBWrapper::GetInstance().value();
    return db->ReadLatestWithResolver(table_name, pk, default_resolver());
}

}  // namespace small::txn
