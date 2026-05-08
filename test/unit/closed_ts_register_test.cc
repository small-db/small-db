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

// Standalone binary: ClosedTsMonotonic registers a fake txn with no
// live coordinator, so the entry can never be drained via Refresh
// and would poison sibling tests sharing the singleton. A separate
// process gives us isolation without a test-only API on the registry.

#include <chrono>
#include <cstdint>

#include "gtest/gtest.h"
#include "src/closedts/registry.h"

namespace {

// T_closed is monotonic: a writer registering after a snapshot ts
// has been advertised as safe must commit strictly above it.
TEST(InFlightRegistry, ClosedTsMonotonic) {
    auto* registry = small::closedts::InFlightRegistry::GetInstance();

    // 1. Advertise `advertised` as safe against an empty registry.
    constexpr int64_t advertised = 2;
    ASSERT_TRUE(registry->WaitUntilSafeToRead(advertised,
                                              std::chrono::milliseconds(50)));

    // 2. Register a writer with a lower_bound below `advertised`.
    int64_t effective = registry->Register(777, 1, "");

    EXPECT_GT(effective, advertised)
        << "T_closed must not regress below an advertised snapshot ts";
}

}  // namespace
