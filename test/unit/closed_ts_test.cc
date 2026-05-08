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
#include <cstdint>
#include <string>

#include "gtest/gtest.h"
#include "src/closedts/registry.h"
#include "src/txn/handle.h"
#include "test/unit/txn_test_fixture.h"

namespace {

using small::test::TxnTestFixture;

// An empty registry permits a reader at any ts to pass immediately.
TEST_F(TxnTestFixture, ClosedTsUnboundedWhenIdle) {
    auto* registry = small::closedts::InFlightRegistry::GetInstance();

    // 1. Wait at an arbitrary ts on an empty registry.
    auto t0 = std::chrono::steady_clock::now();
    bool ok = registry->WaitUntilSafeToRead(2, std::chrono::milliseconds(500));
    auto elapsed = std::chrono::steady_clock::now() - t0;

    EXPECT_TRUE(ok)
        << "empty registry must satisfy WaitUntilSafeToRead at any ts";
    EXPECT_LT(elapsed, std::chrono::milliseconds(50))
        << "fast path on empty registry must not enter the poll loop";
}

// A reader at the writer's write_ts blocks while the writer is in
// flight and unblocks once it commits.
TEST_F(TxnTestFixture, WaitForWriterCommit) {
    auto* registry = small::closedts::InFlightRegistry::GetInstance();

    // 1. Stage an intent. The writer registers at lower_bound = write_ts.
    small::txn::Txn writer;
    ASSERT_TRUE(writer.Begin().ok());
    ASSERT_TRUE(writer
                    .Execute("UPDATE " + unique_table_ +
                             " SET balance = 200 WHERE id = 1")
                    .ok());
    int64_t write_ts = writer.write_ts();

    // 2. While the writer is in flight, T_closed = write_ts - 1.
    EXPECT_FALSE(
        registry->WaitUntilSafeToRead(write_ts, std::chrono::milliseconds(60)))
        << "reader at write_ts must not pass while the writer is in-flight";
    EXPECT_TRUE(registry->WaitUntilSafeToRead(write_ts - 1,
                                              std::chrono::milliseconds(60)))
        << "reader at write_ts - 1 must pass -- no writer is staged below";

    // 3. After commit, T_closed advances past write_ts.
    ASSERT_TRUE(writer.Commit().ok());
    EXPECT_TRUE(
        registry->WaitUntilSafeToRead(write_ts, std::chrono::seconds(1)))
        << "reader at write_ts must pass once the writer has committed";
}

}  // namespace
