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
 * the connection is inside a transaction (explicit `BEGIN`...`COMMIT` or
 * an implicit single-statement wrapper):
 *   - SELECTs use `start_ts` as their snapshot timestamp.
 *   - UPDATEs eagerly write intents to disk tagged with `txn_id`, and
 *     push `commit_ts` upward when the row's latest version is bigger.
 *   - `COMMIT` flips `/_txn/<txn_id>` to COMMITTED in one Put.
 *   - `ROLLBACK` flips it to ABORTED.
 *
 * A new connection starts with `active = false`; the dispatcher wraps
 * any non-BEGIN statement in an implicit single-statement transaction
 * so the protocol applies uniformly.
 */
struct TxnState {
    bool active = false;
    int64_t txn_id = 0;
    int64_t start_ts = 0;
    // Equal to `start_ts` at BEGIN; bumped by the push protocol when a
    // writer encounters a row whose latest committed version_ts >=
    // current commit_ts.
    int64_t commit_ts = 0;
};

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> handle_stmt(
    PgQuery__Node* stmt, TxnState& txn);

}  // namespace small::stmt_handler
