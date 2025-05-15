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

// =====================================================================
// c++ std
// =====================================================================

#include <optional>
#include <string>

// =====================================================================
// third-party libraries
// =====================================================================

// magic_enum
#include "magic_enum/magic_enum.hpp"

// pg_query
#include "pg_query.h"
#include "pg_query.pb-c.h"

// spdlog
#include "spdlog/spdlog.h"

// =====================================================================
// self header
// =====================================================================

#include "src/semantics/check.h"

namespace semantics {

std::optional<std::string> is_string(PgQuery__Node* node) {
    if (node->node_case == PG_QUERY__NODE__NODE_STRING) {
        return node->string->sval;
    } else {
        return std::nullopt;
    }
}

std::string node_type_str(PgQuery__Node* node) {
    return std::string(magic_enum::enum_name(node->node_case));
}

}  // namespace semantics
