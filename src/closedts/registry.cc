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

#include "src/closedts/registry.h"

#include <algorithm>
#include <chrono>
#include <climits>
#include <cstdint>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "spdlog/spdlog.h"
#include "src/txn/txn.h"

namespace small::closedts {

namespace {
constexpr auto kRefreshInterval = std::chrono::milliseconds(20);
}  // namespace

InFlightRegistry* InFlightRegistry::GetInstance() {
    static InFlightRegistry instance;
    return &instance;
}

int64_t InFlightRegistry::Register(int64_t txn_id, int64_t write_ts,
                                   const std::string& coordinator_addr) {
    std::lock_guard<std::mutex> guard(mu_);

    int64_t effective_lb = std::max(write_ts, last_advertised_ts_ + 1);

    auto it = writers_.find(txn_id);
    if (it == writers_.end()) {
        writers_[txn_id] = WriterEntry{effective_lb, coordinator_addr};
    } else if (effective_lb > it->second.lower_bound) {
        // Re-register at a higher value (per-row bump pushed write_ts
        // forward between intents).
        it->second.lower_bound = effective_lb;
    } else {
        // Existing value already covers our effective. Lower bounds
        // never decrease.
        effective_lb = it->second.lower_bound;
    }
    return effective_lb;
}

bool InFlightRegistry::TryAdvertise(int64_t snapshot_ts) {
    std::lock_guard<std::mutex> guard(mu_);
    if (!writers_.empty()) {
        int64_t min_lb = INT64_MAX;
        for (const auto& [_, entry] : writers_) {
            if (entry.lower_bound < min_lb) min_lb = entry.lower_bound;
        }
        if (min_lb - 1 < snapshot_ts) return false;
    }
    last_advertised_ts_ = std::max(last_advertised_ts_, snapshot_ts);
    return true;
}

void InFlightRegistry::RefreshAndDrop() {
    // NOLINTNEXTLINE(build/include_what_you_use) -- <string> is included above
    std::vector<std::pair<int64_t, std::string>> snapshot;
    {
        std::lock_guard<std::mutex> guard(mu_);
        snapshot.reserve(writers_.size());
        for (const auto& [txn_id, entry] : writers_) {
            snapshot.emplace_back(txn_id, entry.coordinator_addr);
        }
    }

    std::vector<int64_t> to_drop;
    for (const auto& [txn_id, addr] : snapshot) {
        auto resp_or = small::txn::resolve_intent(addr, txn_id);
        if (!resp_or.ok()) {
            // Coordinator unreachable; keep entry conservatively. A
            // coordinator that never recovers will block T_closed
            // indefinitely on this node -- coordinator-failure
            // recovery is out of scope for v0.
            SPDLOG_DEBUG("registry refresh: ResolveIntent({}, {}) failed: {}",
                         addr, txn_id, resp_or.status().ToString());
            continue;
        }
        switch (resp_or.value().status()) {
            case small::txn::ResolveIntentResponse::COMMITTED:
            case small::txn::ResolveIntentResponse::ABORTED:
            case small::txn::ResolveIntentResponse::UNKNOWN:
                to_drop.push_back(txn_id);
                break;
            case small::txn::ResolveIntentResponse::ACTIVE:
                break;
            default:
                break;
        }
    }

    std::lock_guard<std::mutex> guard(mu_);
    for (int64_t txn_id : to_drop) writers_.erase(txn_id);
}

bool InFlightRegistry::WaitUntilSafeToRead(int64_t snapshot_ts,
                                           std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    if (TryAdvertise(snapshot_ts)) return true;
    while (true) {
        RefreshAndDrop();
        if (TryAdvertise(snapshot_ts)) return true;
        if (std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(kRefreshInterval);
    }
}

}  // namespace small::closedts
