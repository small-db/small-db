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

#include <fstream>
#include <memory>
#include <string>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

// absl
#include "absl/status/statusor.h"
#include "absl/strings/str_split.h"

// spdlog
#include "spdlog/spdlog.h"

// =====================================================================
// self header
// =====================================================================

#include "test/parser/parser.h"

namespace parser {

// ref:
// https://github.com/cockroachdb/cockroach/blob/1b0a374fd2a101cebcfb24cff4b3b57795ad1df6/pkg/sql/logictest/logic.go#L278
small::type::Type from_sqltest(char c) {
    switch (c) {
        case 'T':
            return small::type::Type::String;
        case 'I':
            return small::type::Type::Int64;
        default:
            SPDLOG_ERROR("unknown type: {}", c);
            return small::type::Type::Int64;
    }
}

std::vector<std::string> split_and_trim(const std::string& input,
                                        char delimiter) {
    std::vector<std::string> result;
    for (absl::string_view part : absl::StrSplit(input, delimiter)) {
        // Trim leading and trailing spaces
        result.push_back(std::string(absl::StripAsciiWhitespace(part)));
    }
    return result;
}

SQLTestUnit::SQLTestUnit(std::vector<std::string> labels, std::string sql,
                         std::string raw_expected,
                         behaviour_t expected_behavior)
    : labels(labels),
      sql(sql),
      raw_expected(raw_expected),
      expected_behavior(expected_behavior) {}

static absl::StatusOr<std::unique_ptr<SQLTestUnit>> init(
    std::vector<std::string> lines) {
    // this first line is tags <tag1> <tag2>
    if (lines.size() < 2) {
        return absl::InternalError(
            "a sql unit must have at least 2 lines");
    }

    std::vector<std::string> tags =
        absl::StrSplit(lines[0], ' ', absl::SkipWhitespace());
    auto sql = lines[1];
    for (int row = 2; row < lines.size(); row++) {
        if (lines[row] == "----") {
            break;
        }
        sql += "\n" + lines[row];
    }

    if (tags.size() != 2) {
        return absl::InternalError(
            "a sql unit must have exactly 2 tags");
    }

    SQLTestUnit::behaviour_t behavior;
    if (tags[0] == "statement" && tags[1] == "ok") {
        // statement ok
        behavior = SQLTestUnit::StatementOK();
    } else if (tags[0] == "query") {
        // query
        auto query = SQLTestUnit::Query();

        // column types
        for (char c : tags[1]) {
            query.column_types.push_back(from_sqltest(c));
        }

        int reply_row_id = -1;
        for (int row = 0; row < lines.size(); row++) {
            if (lines[row] == "----") {
                reply_row_id = 0;
                continue;
            }

            if (reply_row_id < 0) {
                continue;
            }

            switch (reply_row_id) {
                case 0:
                    // column names
                    query.column_names = split_and_trim(lines[row], '|');
                    break;
                case 1:
                    // break line
                    break;
                default:
                    query.expected_output.push_back(
                        split_and_trim(lines[row], '|'));
                    break;
            }

            reply_row_id++;
        }
        behavior = query;
    } else {
        SPDLOG_DEBUG("wrong sql unit");
        for (const auto& tag : tags) {
            SPDLOG_DEBUG("tag: ({})", tag);
        }
        return absl::InternalError("unknown sql unit");
    }

    auto sql_unit = std::make_unique<SQLTestUnit>(tags, sql, "", behavior);
    if (sql_unit->sql.empty()) {
        return absl::InternalError("empty sql");
    }
    return sql_unit;
}

absl::StatusOr<std::vector<SQLTestUnit>> read_sql_test(
    const std::string& sqltest_file) {
    std::vector<SQLTestUnit> sql_tests;
    std::ifstream file(sqltest_file);
    if (!file.is_open()) {
        return absl::NotFoundError(
            absl::StrFormat("failed to open file: %s", sqltest_file));
    }

    std::string line;
    std::vector<std::string> lines;
    while (std::getline(file, line)) {
        if (line.empty()) {
            if (!lines.empty()) {
                auto sql_unit = init(lines);
                if (!sql_unit.ok()) {
                    return sql_unit.status();
                }

                sql_tests.push_back(*sql_unit.value());
                lines.clear();
            }
        } else {
            lines.push_back(line);
        }
    }

    return sql_tests;
}

}  // namespace parser
