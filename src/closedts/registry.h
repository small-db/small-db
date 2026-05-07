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

#include <chrono>
#include <climits>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>

namespace small::closedts {

// Sentinel returned by ComputedClosedTs when the registry has no
// in-flight writers. Treats as "everything is settled, advance freely."
constexpr int64_t kClosedTsUnbounded = INT64_MAX;

// Per-node registry of writers that have written intents on this node
// and haven't been observed in a terminal (COMMITTED / ABORTED) state.
// The closed timestamp on this node is `min(lower_bound) - 1` over the
// registry, or `kClosedTsUnbounded` if empty.
//
// Callers:
//   - WriteIntent path (src/execution/update.cc) calls `Register` when
//     a writer first stages an intent on this node.
//   - SELECT path (src/execution/query.cc) calls `WaitForClosedTs`
//     before scanning, to gate the scan on a settled snapshot.
//
// Cleanup is lazy: `WaitForClosedTs` refreshes by RPC'ing each
// registered writer's coordinator (`ResolveIntent`) and dropping
// entries whose status is no longer ACTIVE. There is no explicit
// `Deregister` from coordinators in this v0 -- a future revision may
// add one as a fast-path optimization.
class InFlightRegistry {
 public:
    // Process-wide singleton.
    static InFlightRegistry* GetInstance();

    // Register a writer that has just staged an intent on this node.
    // Idempotent: if `txn_id` is already registered, update its entry
    // (this can happen when the same txn writes multiple intents on
    // the same node). The lower bound never decreases on re-register;
    // the protocol's invariant (`commit_ts > T_closed`) only requires
    // a lower bound, and bumping the registered lower bound up is
    // always safe.
    void Register(int64_t txn_id, int64_t lower_bound,
                  const std::string& coordinator_addr);

    // Returns `min(lower_bound) - 1` over the current registry, or
    // `kClosedTsUnbounded` if the registry is empty. Does NOT refresh.
    int64_t ComputedClosedTs();

    // Block until `ComputedClosedTs() >= min_ts` or `timeout` elapses.
    //
    // Each iteration: refresh the registry by RPC'ing every registered
    // writer's coordinator; drop entries whose status is COMMITTED,
    // ABORTED, or UNKNOWN; recompute. If still < min_ts, sleep briefly
    // and retry.
    //
    // Returns true if the gate was satisfied within the timeout, false
    // otherwise. The caller decides whether to proceed (e.g. fall
    // through to a stale-but-best-effort scan) or report a retryable
    // error to the client on timeout.
    bool WaitForClosedTs(int64_t min_ts,
                         std::chrono::milliseconds timeout);

 private:
    InFlightRegistry() = default;

    struct WriterEntry {
        int64_t lower_bound = 0;
        std::string coordinator_addr;
    };

    // Self-locking. Snapshot entries, release the lock for the RPCs,
    // re-acquire to apply drops + compute. Returns the post-refresh
    // T_closed.
    int64_t Refresh();

    std::mutex mu_;
    std::map<int64_t, WriterEntry> writers_;
};

}  // namespace small::closedts
