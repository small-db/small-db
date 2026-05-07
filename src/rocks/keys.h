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
#include <string_view>

namespace small::rocks {

// Literal third-segment suffix for an unresolved intent. Sorts above
// every 20-digit zero-padded numeric version_ts, so a prefix scan over
// /<table>/<pk>/ surfaces committed versions first and the intent last.
inline constexpr std::string_view kIntentSuffix = "INTENT";

// "/<table>/" -- prefix for a full-table scan.
std::string TablePrefix(std::string_view table);

// "/<table>/<pk>/" -- prefix for a single-row version scan.
std::string RowPrefix(std::string_view table, std::string_view pk);

// "/<table>/<pk>/<ts>" -- MVCC version key. `ts` is rendered as a
// 20-digit zero-padded decimal so lex order on the suffix matches
// chronological order.
std::string VersionKey(std::string_view table, std::string_view pk,
                       int64_t ts);

// "/<table>/<pk>/INTENT" -- unresolved intent slot.
std::string IntentKey(std::string_view table, std::string_view pk);

// "/_txn/<txn_id>" -- coordinator-side txn record.
std::string TxnKey(int64_t txn_id);

}  // namespace small::rocks
