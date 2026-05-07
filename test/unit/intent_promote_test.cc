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

#include <cstdint>
#include <string>

#include "absl/strings/match.h"
#include "gtest/gtest.h"
#include "src/catalog/catalog.h"
#include "src/rocks/rocks.h"
#include "src/server_info/info.h"
#include "src/txn/handle.h"
#include "test/unit/txn_test_fixture.h"

namespace {

using small::test::TxnTestFixture;

// After a writer commits, the intent slot still holds the new value
// and no numeric version exists at the writer's commit timestamp. The
// first reader to resolve the intent must persist the value as
// /<table>/<pk>/<commit_ts> (half-promote) without deleting the slot.
TEST_F(TxnTestFixture, ReaderHalfPromotesCommittedIntent) {
    auto db = small::rocks::RocksDBWrapper::GetInstance().value();
    const std::string qualified_table = "default_schema." + unique_table_;
    const std::string pk = "1";

    small::txn::Txn writer;
    ASSERT_TRUE(writer.Begin().ok());
    ASSERT_TRUE(writer
                    .Execute("UPDATE " + unique_table_ +
                             " SET balance = 200 WHERE id = 1")
                    .ok());
    ASSERT_TRUE(writer.Commit().ok());
    // Capture write_ts AFTER Commit -- Txn::Commit bumps write_ts to
    // now_ms() (Mechanism A from closed_timestamps.md), so the on-disk
    // commit timestamp is not the value write_ts() would return mid-txn.
    int64_t writer_commit_ts = writer.write_ts();

    // Before any read: intent on disk, latest numeric version is the
    // seed (well below the writer's commit ts).
    EXPECT_TRUE(db->ReadIntent(qualified_table, pk).has_value());
    EXPECT_LT(db->LatestVersionTs(qualified_table, pk), writer_commit_ts);

    small::txn::Txn reader;
    ASSERT_TRUE(reader.Begin().ok());
    auto r = reader.QueryScalar("SELECT balance FROM " + unique_table_ +
                                " WHERE id = 1");
    ASSERT_TRUE(r.ok()) << r.status().ToString();
    EXPECT_EQ(r.value(), "200");
    ASSERT_TRUE(reader.Commit().ok());

    // After the read: numeric version at the writer's commit ts
    // exists; the intent slot is untouched.
    EXPECT_EQ(db->LatestVersionTs(qualified_table, pk), writer_commit_ts);
    EXPECT_TRUE(db->ReadIntent(qualified_table, pk).has_value())
        << "reader must not delete the intent slot";
}

// A writer that finds an ACTIVE intent on its target row aborts with
// a retryable error rather than overwriting it. Plants the ACTIVE
// state directly to mimic what a coordinator crash leaves behind --
// the lock manager would otherwise prevent two live writers from
// reaching this code path on the same row.
TEST_F(TxnTestFixture, WriterAbortsOnActiveIntent) {
    auto db = small::rocks::RocksDBWrapper::GetInstance().value();
    const std::string qualified_table = "default_schema." + unique_table_;
    const std::string pk = "1";
    constexpr int64_t kStaleTxnId = 999'999;

    auto info = small::server_info::get_info().value();
    db->WriteTxnRecord(kStaleTxnId,
                       small::rocks::TxnRecord{
                           small::rocks::TxnStatus::ACTIVE,
                           /*start_ts=*/1, /*write_ts=*/1, {}});
    auto table = small::catalog::CatalogManager::GetInstance()
                     ->GetTable(qualified_table)
                     .value();
    db->WriteIntent(table, pk, /*values=*/{"1", "999"}, kStaleTxnId,
                    info->grpc_addr);

    small::txn::Txn writer;
    ASSERT_TRUE(writer.Begin().ok());
    auto upd = writer.Execute("UPDATE " + unique_table_ +
                              " SET balance = 300 WHERE id = 1");
    ASSERT_FALSE(upd.ok()) << "writer should not proceed past an ACTIVE intent";
    EXPECT_TRUE(absl::StrContains(upd.status().message(), "active intent"))
        << upd.status().ToString();
    ASSERT_TRUE(writer.Rollback().ok());

    // The planted intent and txn record are still present -- the
    // writer must not have promoted or deleted them.
    EXPECT_TRUE(db->ReadIntent(qualified_table, pk).has_value());
    auto record = db->ReadTxnRecord(kStaleTxnId);
    ASSERT_TRUE(record.has_value());
    EXPECT_EQ(record->status, small::rocks::TxnStatus::ACTIVE);
}

}  // namespace
