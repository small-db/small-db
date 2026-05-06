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

// =====================================================================
// c++ std
// =====================================================================

#include <cstdint>
#include <string>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

// absl
#include "absl/status/statusor.h"

// arrow
#include "arrow/api.h"

// pg_query
#include "pg_query.pb-c.h"

namespace small::stmt_handler {

/**
 * @brief Per-connection transaction state.
 *
 * Each TCP client connection holds one of these. While `active` is true,
 * the connection is inside a `BEGIN`...`COMMIT` block:
 *   - SELECTs use `start_ts` as their snapshot timestamp (instead of now).
 *   - UPDATEs are *buffered* into `pending_updates` rather than executed
 *     immediately. The actual dispatch happens at `COMMIT` time, with a
 *     single shared `commit_ts` so all writes appear atomically.
 * `COMMIT` flushes the buffer; `ROLLBACK` discards it. A new connection
 * starts with `active = false` (auto-commit per statement).
 */
struct TxnState {
    bool active = false;
    int64_t start_ts = 0;

    // Each entry is the packed bytes of a PgQuery__UpdateStmt, captured
    // at UPDATE time so the AST can outlive the parser's allocation.
    std::vector<std::string> pending_updates;
};

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> handle_stmt(
    PgQuery__Node* stmt, TxnState& txn);

}  // namespace small::stmt_handler
