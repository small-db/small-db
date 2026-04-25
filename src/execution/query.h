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

// Run a SELECT.
//
// dispatch=true:  fan out to every node, each running with the provided
//                 read_ts as its snapshot, then concatenate results.
// dispatch=false: run locally with read_ts as the snapshot ts.
//
// read_ts_millis = 0 falls back to "use now()" — i.e., observe the latest
// committed state. Pass an explicit value to read at a snapshot, e.g. so
// reads inside a transaction don't see writes committed after BEGIN.
absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> query(
    PgQuery__SelectStmt* select_stmt, bool dispatch,
    int64_t read_ts_millis = 0);

class QueryServiceImpl final : public small::execution::Query::Service {
   public:
    grpc::Status Query(grpc::ServerContext* context,
                       const small::execution::RawNode* request,
                       small::execution::QueryResponse* response) final;
};

}  // namespace small::execution
