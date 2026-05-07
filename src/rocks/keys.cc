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

#include "src/rocks/keys.h"

#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>

#include "absl/strings/str_format.h"

namespace small::rocks {

std::string TablePrefix(std::string_view table) {
    return absl::StrFormat("/%s/", table);
}

std::string RowPrefix(std::string_view table, std::string_view pk) {
    return absl::StrFormat("/%s/%s/", table, pk);
}

std::string VersionKey(std::string_view table, std::string_view pk,
                       int64_t ts) {
    std::ostringstream ts_str;
    ts_str << std::setw(20) << std::setfill('0') << ts;
    return absl::StrFormat("/%s/%s/%s", table, pk, ts_str.str());
}

std::string IntentKey(std::string_view table, std::string_view pk) {
    return absl::StrFormat("/%s/%s/%s", table, pk, kIntentSuffix);
}

std::string TxnKey(int64_t txn_id) {
    return absl::StrFormat("/_txn/%d", txn_id);
}

}  // namespace small::rocks
