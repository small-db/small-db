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
#include <thread>

#include "gtest/gtest.h"
#include "src/closedts/registry.h"
#include "src/server_info/info.h"
#include "src/txn/handle.h"
#include "test/unit/txn_test_fixture.h"

namespace {

using small::test::TxnTestFixture;

// With no in-flight writers on this node, T_closed is unbounded
// and a reader's WaitForClosedTs returns immediately at any
// snapshot_ts.
TEST_F(TxnTestFixture, ClosedTsUnboundedWhenIdle) {
    auto* registry = small::closedts::InFlightRegistry::GetInstance();
    EXPECT_EQ(registry->ComputedClosedTs(),
              small::closedts::kClosedTsUnbounded);

    auto t0 = std::chrono::steady_clock::now();
    bool ok = registry->WaitForClosedTs(/*min_ts=*/9'000'000'000'000,
                                        std::chrono::milliseconds(500));
    auto elapsed = std::chrono::steady_clock::now() - t0;

    EXPECT_TRUE(ok);
    EXPECT_LT(elapsed, std::chrono::milliseconds(50))
        << "WaitForClosedTs should return immediately on an idle registry";
}

// With an in-flight writer registered at lower_bound L, T_closed
// is L - 1 and a reader at min_ts >= L blocks until the writer
// commits or aborts. After commit, lazy refresh drops the entry
// and T_closed advances; the read unblocks.
TEST_F(TxnTestFixture, ClosedTsBlocksUntilWriterFinishes) {
    auto* registry = small::closedts::InFlightRegistry::GetInstance();
    auto info = small::server_info::get_info().value();

    // Run a writer (UPDATE) that registers itself in the registry,
    // then commits in another thread after a short delay. The reader
    // thread waits for T_closed to advance past the writer's
    // lower_bound; it should block then unblock.
    small::txn::Txn writer;
    ASSERT_TRUE(writer.Begin().ok());
    ASSERT_TRUE(writer
                    .Execute("UPDATE " + unique_table_ +
                             " SET balance = 200 WHERE id = 1")
                    .ok());
    int64_t writer_lower_bound = writer.write_ts();

    // The registry should now contain this writer.
    int64_t closed_during =
        small::closedts::InFlightRegistry::GetInstance()->ComputedClosedTs();
    EXPECT_EQ(closed_during, writer_lower_bound - 1)
        << "T_closed should equal lower_bound - 1 with one in-flight writer";

    // Spawn a thread that commits the writer after a short delay.
    std::thread committer([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        ASSERT_TRUE(writer.Commit().ok());
    });

    auto t0 = std::chrono::steady_clock::now();
    bool ok = registry->WaitForClosedTs(writer_lower_bound,
                                        std::chrono::seconds(5));
    auto elapsed = std::chrono::steady_clock::now() - t0;
    committer.join();

    EXPECT_TRUE(ok) << "WaitForClosedTs should unblock once the writer commits";
    EXPECT_GE(elapsed, std::chrono::milliseconds(80))
        << "WaitForClosedTs returned before the writer's commit";
    EXPECT_LT(elapsed, std::chrono::seconds(2))
        << "WaitForClosedTs took longer than the writer's commit + refresh";
}

// A writer's commit_ts is bumped to now_ms() at Commit time
// (Mechanism A). A reader whose snapshot was taken before the
// writer began its commit cannot see the writer's effects, even
// if the writer's start_ts < reader.snapshot_ts.
TEST_F(TxnTestFixture, CommitTsBumpedAtCommit) {
    small::txn::Txn writer;
    ASSERT_TRUE(writer.Begin().ok());
    int64_t start_ts = writer.start_ts();

    ASSERT_TRUE(writer
                    .Execute("UPDATE " + unique_table_ +
                             " SET balance = 200 WHERE id = 1")
                    .ok());

    // Pause so wall clock advances meaningfully past start_ts.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    ASSERT_TRUE(writer.Commit().ok());
    int64_t commit_ts = writer.write_ts();

    EXPECT_GT(commit_ts, start_ts)
        << "Mechanism A should bump commit_ts above start_ts at Commit time";
    EXPECT_GE(commit_ts - start_ts, 50)
        << "commit_ts should reflect the actual wall clock at Commit";
}

}  // namespace
