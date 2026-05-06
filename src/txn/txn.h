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
#include <optional>
#include <string>

// =====================================================================
// third-party libraries
// =====================================================================

#include "absl/status/statusor.h"
#include "grpcpp/grpcpp.h"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/txn/txn.grpc.pb.h"
#include "src/txn/txn.pb.h"

namespace small::txn {

// Server-side handler. Looks up /_txn/<txn_id> in the *local* RocksDB
// and returns its status. The client must RPC the coordinator that
// owns the txn record (the coordinator_addr embedded in the intent),
// not an arbitrary node; an RPC to the wrong server returns UNKNOWN.
class TxnServiceImpl final : public TxnService::Service {
 public:
    grpc::Status ResolveIntent(grpc::ServerContext* context,
                               const ResolveIntentRequest* request,
                               ResolveIntentResponse* response) override;
};

// Client-side helper. Single-shot RPC to coordinator_addr asking for
// txn_id's status. Returns an error if the channel can't be reached;
// otherwise returns the response (which may carry UNKNOWN if the
// coordinator has no record for that txn_id).
absl::StatusOr<ResolveIntentResponse> resolve_intent(
    const std::string& coordinator_addr, int64_t txn_id);

// Largest committed `version_ts` on (table_name, pk), considering both
// numeric committed versions on disk *and* any unresolved INTENT key
// whose backing transaction has already committed. RPCs the
// coordinator embedded in the intent to learn its status.
//
// Used by the write path's push protocol: a writer about to bump its
// commit_ts compares against this value, not the rocks-layer view
// that ignores intents.
//
// Layering note: this lives in src/txn/ rather than as a method on
// RocksDBWrapper because resolution requires gRPC, and rocks/ is kept
// network-free.
absl::StatusOr<int64_t> latest_committed_version_ts(
    const std::string& table_name, const std::string& pk);

// Intent-aware read of an entire table at a snapshot. For each pk,
// surfaces the largest of (numeric latest committed version_ts <=
// snapshot_ts) and (commit_ts of an unresolved INTENT for a COMMITTED
// txn whose commit_ts <= snapshot_ts). RPCs each intent's coordinator
// to resolve.
std::map<std::string, std::map<std::string, std::string>>
read_table_at_snapshot(const std::string& table_name, int64_t snapshot_ts);

// Intent-aware read of a single row's latest committed value. Same
// rule as `read_table_at_snapshot` but scoped to one (table, pk).
// Used by writers in their pre-image read so the SET clause computes
// on top of any prior COMMITTED-but-unpromoted intent.
std::optional<std::map<std::string, std::string>> read_latest_with_intents(
    const std::string& table_name, const std::string& pk);

}  // namespace small::txn
