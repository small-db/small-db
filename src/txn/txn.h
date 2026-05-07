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

// Combined writer-side pre-image read for (table_name, pk). Returns
// the value to feed into the SET clause AND the largest committed
// `version_ts` on the row, both observed under one prefix scan and at
// most one ResolveIntent RPC.
//
// Resolves the intent (if any) under writer-mode semantics:
//   COMMITTED -- full-promote (atomic Put numeric version + Delete
//                INTENT). Treats the resolved commit_ts as a candidate
//                for the latest, and the intent's value as a candidate
//                for the pre-image.
//   ABORTED / UNKNOWN -- skip the intent entirely.
//   ACTIVE -- return AbortedError. A concurrent writer has staged its
//             intent but hasn't committed yet; the caller must roll
//             back and retry.
//
// Caller MUST hold lock(table, pk). The full-promote is path-addressed
// and would race with concurrent slot mutation if no lock were held.
//
// Returns nullopt if the row doesn't exist on this node (the caller is
// not the partition owner); the writer's update path uses this as the
// signal to no-op without writing an intent.
//
// Replaces the older read_latest_with_intents + latest_committed_version_ts
// pair, which made two scans + two RPCs and had a TOCTOU window between
// them where a prior writer could transition ACTIVE -> COMMITTED.
struct WriterPreimage {
    // Pre-image value: the latest committed row contents at the moment
    // of the read. Already includes the resolved intent's value if
    // that intent was COMMITTED with the largest commit_ts.
    std::map<std::string, std::string> values;
    // Largest committed `version_ts` on this row, used by the per-row
    // bump rule in src/execution/update.cc.
    int64_t latest_committed_ts;
};

absl::StatusOr<std::optional<WriterPreimage>> read_for_writer(
    const std::string& table_name, const std::string& pk);

// Intent-aware read of an entire table at a snapshot. For each pk,
// surfaces the largest of (numeric latest committed version_ts <=
// snapshot_ts) and (commit_ts of an unresolved INTENT for a COMMITTED
// txn whose commit_ts <= snapshot_ts). RPCs each intent's coordinator
// to resolve.
std::map<std::string, std::map<std::string, std::string>>
read_table_at_snapshot(const std::string& table_name, int64_t snapshot_ts);

}  // namespace small::txn
