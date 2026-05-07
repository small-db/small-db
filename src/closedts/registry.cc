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

#include <chrono>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/strings/str_format.h"
#include "spdlog/spdlog.h"

#include "src/txn/txn.h"

#include "src/closedts/registry.h"

namespace small::closedts {

namespace {
// How long to sleep between refresh attempts during WaitUntilSafeToRead.
constexpr auto kRefreshInterval = std::chrono::milliseconds(20);
}  // namespace

InFlightRegistry* InFlightRegistry::GetInstance() {
    static InFlightRegistry instance;
    return &instance;
}

int64_t InFlightRegistry::Register(int64_t txn_id, int64_t lower_bound,
                                   const std::string& coordinator_addr) {
    std::lock_guard<std::mutex> guard(mu_);

    // Compute current min(lower_bound) inline -- ComputedClosedTs would
    // re-acquire the lock. The protocol invariant requires the writer's
    // effective lower_bound be > T_closed = min_lb - 1, i.e. >= min_lb.
    // If the request is below min_lb, T_closed has already advanced
    // past the writer's start_ts and we must bump.
    int64_t min_lb = INT64_MAX;
    for (const auto& [_, entry] : writers_) {
        if (entry.lower_bound < min_lb) min_lb = entry.lower_bound;
    }

    int64_t effective_lb = lower_bound;
    if (min_lb != INT64_MAX && lower_bound < min_lb) {
        effective_lb = min_lb;
    }

    auto it = writers_.find(txn_id);
    if (it == writers_.end()) {
        writers_[txn_id] = WriterEntry{effective_lb, coordinator_addr};
    } else if (effective_lb > it->second.lower_bound) {
        // Re-register with a higher value (e.g. caller's per-row bump
        // pushed write_ts forward between intents).
        it->second.lower_bound = effective_lb;
    } else {
        // Re-register where the existing value is already >= our
        // effective. Keep the existing value -- lower bounds never
        // decrease.
        effective_lb = it->second.lower_bound;
    }
    return effective_lb;
}

int64_t InFlightRegistry::ComputedClosedTs() {
    std::lock_guard<std::mutex> guard(mu_);
    if (writers_.empty()) return kClosedTsUnbounded;
    int64_t min_lb = INT64_MAX;
    for (const auto& [txn_id, entry] : writers_) {
        if (entry.lower_bound < min_lb) min_lb = entry.lower_bound;
    }
    return min_lb - 1;
}

int64_t InFlightRegistry::Refresh() {
    // Snapshot entries under the lock, release for RPCs, re-acquire
    // to apply drops + compute the result.
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
            // Coordinator unreachable; keep entry conservatively. The
            // caller's retry loop will try again. A coordinator that
            // never recovers will block T_closed indefinitely on this
            // node -- coordinator-failure recovery is out of scope for
            // v0.
            SPDLOG_DEBUG("registry refresh: ResolveIntent({}, {}) failed: {}",
                         addr, txn_id, resp_or.status().ToString());
            continue;
        }
        const auto& resp = resp_or.value();
        switch (resp.status()) {
            case small::txn::ResolveIntentResponse::COMMITTED:
            case small::txn::ResolveIntentResponse::ABORTED:
            case small::txn::ResolveIntentResponse::UNKNOWN:
                to_drop.push_back(txn_id);
                break;
            case small::txn::ResolveIntentResponse::ACTIVE:
                // Still in flight. Keep.
                break;
            default:
                break;
        }
    }

    std::lock_guard<std::mutex> guard(mu_);
    for (int64_t txn_id : to_drop) writers_.erase(txn_id);
    if (writers_.empty()) return kClosedTsUnbounded;
    int64_t min_lb = INT64_MAX;
    for (const auto& [txn_id, entry] : writers_) {
        if (entry.lower_bound < min_lb) min_lb = entry.lower_bound;
    }
    return min_lb - 1;
}

bool InFlightRegistry::WaitUntilSafeToRead(
    int64_t snapshot_ts, std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;

    // Fast path: no in-flight writers blocking us.
    if (ComputedClosedTs() >= snapshot_ts) return true;

    while (true) {
        if (Refresh() >= snapshot_ts) return true;
        if (std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(kRefreshInterval);
    }
}

}  // namespace small::closedts
