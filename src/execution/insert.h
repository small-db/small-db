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
#include "absl/status/status.h"

// =====================================================================
// protobuf generated files
// =====================================================================

#include "src/execution/execution.grpc.pb.h"
#include "src/execution/execution.pb.h"

namespace small::execution {

absl::Status insert(PgQuery__InsertStmt* insert_stmt);

class InsertServiceImpl final : public small::execution::Insert::Service {
   public:
    grpc::Status Insert(grpc::ServerContext* context,
                        const small::execution::Row* request,
                        small::execution::InsertReply* response) final;
};

}  // namespace small::execution
