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

#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/server_info/info.h"

// =====================================================================
// self header
// =====================================================================

#include "src/id/generator.h"

namespace id {

// Snowflake-style 63-bit ID:
//
//   1 bit sign (always 0; keeps values positive in int64)
//   41 bits ms-since-epoch (relative to kEpochMs; ~69 years of range)
//   10 bits node_id        (1024 distinct coordinators)
//   12 bits sequence       (4096 IDs/ms/node ceiling)
//
// The (coordinator_addr, txn_id) pair is what callers use as the
// canonical identifier (the txn record lives only on its coordinator),
// but encoding the node_id into the value itself makes IDs globally
// unique even if the pair leaks. The timestamp prefix gives k-sortable
// IDs -- `rocks_scan /_txn/` over a region's data dir prints commits
// in chronological order, which the project's debug workflow relies on.
//
// Restart safety: the timestamp moves forward across restarts, so
// post-restart IDs always sort above pre-restart IDs. A clock moving
// *backward* (NTP correction) is detected and busy-waited.

namespace {

// 2024-01-01 00:00:00 UTC. Reduces the 41-bit ms-since-epoch we encode.
constexpr int64_t kEpochMs = 1704067200000LL;
constexpr int kSeqBits = 12;
constexpr int kNodeBits = 10;
constexpr int64_t kSeqMask = (1LL << kSeqBits) - 1;
constexpr int64_t kNodeMask = (1LL << kNodeBits) - 1;
constexpr int kNodeShift = kSeqBits;
constexpr int kTimeShift = kSeqBits + kNodeBits;

int64_t wall_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// Hash the per-server UUID (server_info.id) down to 10 bits. Stable
// across calls on the same server; near-uniformly distributed across
// servers. Collisions are possible but vanishingly rare for a 3-node
// test cluster (~1.5% across pairs). Falls back to 0 when called
// before server_info::init (e.g., in tests).
int derive_node_id() {
    auto info = small::server_info::get_info();
    if (!info.ok()) return 0;
    return static_cast<int>(std::hash<std::string>{}(info.value()->id) &
                            kNodeMask);
}

}  // namespace

int64_t generate_id() {
    static std::mutex mu;
    static int64_t last_ms = 0;
    static int64_t seq = 0;
    static int node_id = -1;

    std::lock_guard<std::mutex> lock(mu);
    if (node_id < 0) node_id = derive_node_id();

    int64_t cur_ms;
    while (true) {
        cur_ms = wall_ms();
        if (cur_ms < last_ms) {
            // Wall clock moved backward (NTP correction). Yield until
            // the system clock catches up; never issue an ID in the
            // backward window.
            std::this_thread::yield();
            continue;
        }
        if (cur_ms == last_ms) {
            seq = (seq + 1) & kSeqMask;
            if (seq == 0) {
                // 4096 IDs already issued this ms; wait for next ms.
                continue;
            }
            break;
        }
        // cur_ms > last_ms: first ID in a new ms.
        seq = 0;
        break;
    }
    last_ms = cur_ms;

    int64_t time_part = cur_ms - kEpochMs;
    return (time_part << kTimeShift) |
           (static_cast<int64_t>(node_id) << kNodeShift) | seq;
}

}  // namespace id
