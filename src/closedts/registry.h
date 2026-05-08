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
#include <cstdint>
#include <map>
#include <mutex>
#include <string>

namespace small::closedts {

// Per-node registry of in-flight writers, gating the local closed
// timestamp.
class InFlightRegistry {
   public:
    // Process-wide singleton.
    static InFlightRegistry* GetInstance();

    // Register a writer's write_ts on this node. Returns the resulting
    // write_ts, bumped above the last advertised closed timestamp if the
    // provided value is at or below it.
    int64_t Register(int64_t txn_id, int64_t write_ts,
                     const std::string& coordinator_addr);

    // Wait until all writes at or before `snapshot_ts` are settled on
    // this node.
    //
    // On true, scans at `snapshot_ts` on this node are stable: no
    // writer registered after this call will commit at or before
    // `snapshot_ts`.
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

    bool TryAdvertise(int64_t snapshot_ts);

    // Resolve every registered writer's intent and drop entries no
    // longer ACTIVE. Self-locking; releases the lock for the RPCs.
    void RefreshAndDrop();

    std::mutex mu_;

    // All active write transactions with intents on this node.
    std::map<int64_t, WriterEntry> writers_;

    // monotonically increasing
    int64_t last_advertised_ts_ = 0;
};

}  // namespace small::closedts
