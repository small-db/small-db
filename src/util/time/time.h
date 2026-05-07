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
#include <cstdio>
#include <ctime>
#include <string>

namespace small::util {

// Render a millisecond unix timestamp as "YYYY-MM-DD HH:MM:SS.mmm" in local
// time. 0 and negative values are treated as the project's "unset" sentinel
// and rendered as "unset" so logs do not show a misleading 1970-01-01.
inline std::string FormatTsMs(int64_t ms) {
    if (ms <= 0) return "unset";
    auto s = static_cast<time_t>(ms / 1000);
    int ms_part = static_cast<int>(ms % 1000);
    struct tm tm {};
    localtime_r(&s, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
    char out[48];
    std::snprintf(out, sizeof(out), "%s.%03d", buf, ms_part);
    return out;
}

}  // namespace small::util
