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

// Per-session transaction state. One instance per connection, owned by the
// connection-tracking layer (see SocketsManager in server.cc).
//
// Lifecycle:
//   - constructed inactive when the connection is established
//   - BEGIN: active=true, read_ts pinned to now()
//   - UPDATE while active: append the packed UpdateStmt bytes to writes
//     instead of dispatching immediately
//   - COMMIT: pick commit_ts, dispatch each buffered write with that ts
//     (so all writes from this txn share one timestamp), reset state
//   - ROLLBACK / connection close: reset state, discard writes
struct TxnState {
    bool active = false;
    int64_t read_ts = 0;
    std::vector<std::vector<uint8_t>> writes;
};

struct SessionState {
    TxnState txn;
};

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> handle_stmt(
    PgQuery__Node* stmt, SessionState& session);

}  // namespace small::stmt_handler
