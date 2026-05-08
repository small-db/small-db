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
#include <string>

// =====================================================================
// third-party libraries
// =====================================================================

// pg_query
#include "pg_query.pb-c.h"

// absl
#include "absl/status/statusor.h"

// arrow
#include "arrow/api.h"

// =====================================================================
// small-db libraries (protobuf generated)
// =====================================================================

#include "src/execution/execution.grpc.pb.h"
#include "src/execution/execution.pb.h"

namespace small::execution {

// Returned from `update`. `final_write_ts` is the timestamp the owner
// used when it wrote the intent (>= the caller's write_ts; bigger if
// the row's chain forced a push). `intent_key` is the /<table>/<pk>/
// INTENT key the caller appends to /_txn/<txn_id>.intent_keys[].
struct UpdateResult {
    int64_t final_write_ts = 0;
    std::string intent_key;
};

// Coordinator-side entry point. Fans out to every peer's UpdateService;
// the row owner runs the intent path. The caller is responsible for
// persisting any push to /_txn/<txn_id> and for appending intent_key.
absl::StatusOr<UpdateResult> update(PgQuery__UpdateStmt* update_stmt,
                                    bool dispatch, int64_t write_ts,
                                    int64_t txn_id,
                                    const std::string& coordinator_addr);

class UpdateServiceImpl final : public small::execution::Update::Service {
   public:
    grpc::Status Update(grpc::ServerContext* context,
                        const small::execution::RawNode* request,
                        small::execution::WriteResponse* response) final;
};

}  // namespace small::execution
