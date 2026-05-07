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

// Sentinel for an empty registry. Treats as "everything is settled,
// advance freely."
constexpr int64_t kClosedTsUnbounded = INT64_MAX;

// Per-node registry of writers that have written intents on this node
// and haven't been observed in a terminal (COMMITTED / ABORTED) state.
// The closed timestamp on this node is `min(lower_bound) - 1` over the
// registry, or `kClosedTsUnbounded` if empty.
//
// Callers:
//   - WriteIntent path (src/execution/update.cc) calls `Register` when
//     a writer first stages an intent on this node.
//   - SELECT path (src/execution/query.cc) calls `WaitUntilSafeToRead`
//     before scanning, to gate the scan on a settled snapshot.
//
// Cleanup is lazy: `WaitUntilSafeToRead` refreshes by RPC'ing each
// registered writer's coordinator (`ResolveIntent`) and dropping
// entries whose status is no longer ACTIVE. There is no explicit
// `Deregister` from coordinators in this v0 -- a future revision may
// add one as a fast-path optimization.
class InFlightRegistry {
   public:
    // Process-wide singleton.
    static InFlightRegistry* GetInstance();

    // Register a writer that has just staged an intent on this node.
    //
    // The protocol invariant -- "every writer's commit_ts > T_closed
    // at register time" -- requires that the writer's lower_bound be
    // strictly greater than T_closed (= min(lower_bound) - 1) on this
    // node. If the requested lower_bound would violate that (the writer
    // arrived after T_closed has already advanced past it), the
    // registry bumps the entry to the smallest valid value -- i.e.,
    // the current min lower_bound, which is T_closed + 1.
    //
    // Returns the **effective** lower_bound the registry stored. If
    // the return value is greater than the requested value, the writer
    // was bumped; the caller MUST propagate the new value back to the
    // coordinator's write_ts (otherwise the writer could later commit
    // at a ts <= T_closed, breaking the closed-ts gate for any reader
    // that's already passed).
    //
    // Idempotent: re-registering the same txn_id never decreases its
    // stored lower_bound; the existing value is kept if it's already
    // >= the new effective value.
    int64_t Register(int64_t txn_id, int64_t lower_bound,
                     const std::string& coordinator_addr);

    // Wait until all writes at or before `snapshot_ts` are settled on
    // this node.
    //
    // On true, scans at `snapshot_ts` on this node are stable: no
    // in-flight writer will later commit at or before that timestamp.
    //
    // Returns false on timeout.
    bool WaitUntilSafeToRead(int64_t snapshot_ts,
                             std::chrono::milliseconds timeout);

   private:
    InFlightRegistry() = default;

    struct WriterEntry {
        int64_t lower_bound = 0;
        std::string coordinator_addr;
    };

    // `min(lower_bound) - 1` over the current registry, or
    // `kClosedTsUnbounded` if empty. Does NOT refresh.
    int64_t ComputedClosedTs();

    // Self-locking. Snapshot entries, release the lock for the RPCs,
    // re-acquire to apply drops + compute. Returns the post-refresh
    // T_closed.
    int64_t Refresh();

    std::mutex mu_;

    // All active write transactions with intents on this node.
    //
    // - Writers added by write action on staging an intent.
    // - Writers cleaned lazily by read action on waiting for closed ts.
    std::map<int64_t, WriterEntry> writers_;
};

}  // namespace small::closedts
