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
// local libraries
// =====================================================================

#include "src/type/type.h"

// =====================================================================
// self header
// =====================================================================

#include "src/semantics/extract.h"

namespace small::semantics {

std::optional<small::type::Datum> extract_const(PgQuery__AConst* node) {
    switch (node->val_case) {
        case PG_QUERY__A__CONST__VAL_SVAL: {
            return small::type::Datum(node->sval->sval);
        }
        case PG_QUERY__A__CONST__VAL_IVAL: {
            return small::type::Datum(static_cast<int64_t>(node->ival->ival));
        }
        default: {
            SPDLOG_ERROR("unknown const type, node_case: {}",
                         magic_enum::enum_name(node->val_case));
            return std::nullopt;
        }
    }
}

}  // namespace small::semantics
