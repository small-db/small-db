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

#include <string>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

// absl
#include "absl/status/statusor.h"

// =====================================================================
// local libraries
// =====================================================================

#include "src/type/type.h"

#pragma once

namespace parser {

class SQLTestUnit {
   public:
    std::vector<std::string> labels;
    std::string sql;
    std::string raw_expected;

    class StatementOK {};

    class Query {
       public:
        std::vector<std::string> column_names;
        std::vector<small::type::Type> column_types;
        std::vector<std::vector<std::string>> expected_output;
    };

    using behaviour_t = std::variant<StatementOK, Query>;

    behaviour_t expected_behavior;

    SQLTestUnit(std::vector<std::string> labels, std::string sql,
                std::string raw_expected, behaviour_t expected_behavior);

    static absl::StatusOr<std::unique_ptr<SQLTestUnit>> init(
        std::vector<std::string> lines);
};

absl::StatusOr<std::vector<SQLTestUnit>> read_sql_test(
    const std::string& sqltest_file);

}  // namespace parser
