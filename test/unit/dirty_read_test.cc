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

#include <string_view>

#include "gtest/gtest.h"
#include "src/txn/handle.h"
#include "test/unit/txn_test_fixture.h"

namespace {

using small::test::TxnTestFixture;

// One transaction must never see another transaction's uncommitted
// writes; once that transaction commits, subsequent reads see them.
TEST_F(TxnTestFixture, DirtyRead) {
    auto expect_balance = [&](std::string_view want, std::string_view why) {
        small::txn::Txn t;
        auto r = t.QueryScalar("SELECT balance FROM " + unique_table_ +
                               " WHERE id = 1");
        ASSERT_TRUE(r.ok()) << r.status().ToString();
        EXPECT_EQ(r.value(), want) << why;
    };

    small::txn::Txn writer;
    ASSERT_TRUE(writer.Begin().ok());
    ASSERT_TRUE(writer
                    .Execute("UPDATE " + unique_table_ +
                             " SET balance = 200 WHERE id = 1")
                    .ok());

    expect_balance("100", "uncommitted write must not be visible");
    ASSERT_TRUE(writer.Commit().ok());
    expect_balance("200", "committed write must be visible");
}

}  // namespace
