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

#include "gtest/gtest.h"
#include "src/txn/handle.h"
#include "test/unit/txn_test_fixture.h"

namespace {

using small::test::TxnTestFixture;

// Dirty read: while a writer's UPDATE is uncommitted, a concurrent
// reader must NOT see the new value -- visibility hinges on the
// writer's txn-record status (ACTIVE), not on the snapshot timestamp.
// After the writer commits, a later reader must see the new value.
//
// Sequential Begin() calls give us t_writer < t_reader1 < t_reader2,
// which is the only ordering this test relies on.
TEST_F(TxnTestFixture, DirtyRead) {
    small::txn::Txn writer;
    ASSERT_TRUE(writer.Begin().ok());
    auto upd = writer.Execute("UPDATE " + unique_table_ +
                              " SET balance = 200 WHERE id = 1");
    ASSERT_TRUE(upd.ok()) << upd.status().ToString();
    // Writer has NOT committed yet.

    // Reader1 sees an ACTIVE intent. Per dirty-read semantics it must
    // return the prior committed value (100), not the intent's 200.
    small::txn::Txn reader1;
    ASSERT_TRUE(reader1.Begin().ok());
    auto r1 = reader1.QueryScalar("SELECT balance FROM " + unique_table_ +
                                  " WHERE id = 1");
    ASSERT_TRUE(r1.ok()) << r1.status().ToString();
    EXPECT_EQ(r1.value(), "100")
        << "dirty read: reader saw uncommitted intent value";
    ASSERT_TRUE(reader1.Commit().ok());

    // Writer commits. The intent is now a visible committed version.
    ASSERT_TRUE(writer.Commit().ok());

    // Reader2 at a later snapshot must see the new value.
    small::txn::Txn reader2;
    ASSERT_TRUE(reader2.Begin().ok());
    auto r2 = reader2.QueryScalar("SELECT balance FROM " + unique_table_ +
                                  " WHERE id = 1");
    ASSERT_TRUE(r2.ok()) << r2.status().ToString();
    EXPECT_EQ(r2.value(), "200")
        << "post-commit reader did not see the committed value";
    ASSERT_TRUE(reader2.Commit().ok());
}

}  // namespace
