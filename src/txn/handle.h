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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arrow/api.h"
#include "pg_query.pb-c.h"

namespace small::txn {

// Drives one transaction's worth of work: lifecycle (Begin/Commit/
// Rollback) plus statement execution under that lifecycle.
//
// A statement issued while the txn is inactive runs inside an implicit
// single-statement transaction (Begin before, Commit after). Explicit
// BEGIN/COMMIT/ROLLBACK toggle the active flag between statements.
class Txn {
   public:
    Txn() = default;

    // Parse `sql` and run the first statement under this transaction.
    // Empty input or parser errors return an error status.
    absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> Execute(
        std::string_view sql);

    // Run a pre-parsed AST node under this transaction. Same semantics
    // as Execute().
    absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> ExecuteNode(
        PgQuery__Node* stmt);

    // Run a SELECT and return the string form of the first cell of the
    // first row. Errors if the query returns no rows or no columns.
    absl::StatusOr<std::string> QueryScalar(std::string_view sql);

    absl::Status Begin();
    absl::Status Commit();
    absl::Status Rollback();

    [[nodiscard]] bool active() const { return active_; }

    // Mutable: may advance on UPDATE, promoted to the final commit
    // timestamp at COMMIT.
    [[nodiscard]] int64_t write_ts() const { return write_ts_; }

   private:
    bool active_ = false;
    int64_t txn_id_ = 0;
    int64_t start_ts_ = 0;
    int64_t write_ts_ = 0;
};

}  // namespace small::txn
