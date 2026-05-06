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

#include <string>

// =====================================================================
// third-party libraries
// =====================================================================

// absl
#include "absl/status/statusor.h"

// pg_query
#include "pg_query.pb-c.h"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/type/type.pb.h"

namespace small::semantics {

std::optional<small::type::Datum> extract_const(PgQuery__AConst* node);

// Extract a libpg_query A_Const literal as its canonical string form,
// regardless of which oneof branch (`ival`, `sval`, ...) is set. Used
// by handlers that store partition values, CHECK constants, etc., as
// strings in the catalog.
//
// Returns InvalidArgumentError for kinds we don't yet support
// (FVAL/BOOLVAL/BSVAL/NOT_SET) -- callers should propagate.
absl::StatusOr<std::string> a_const_to_string(PgQuery__AConst* c);

}  // namespace small::semantics
