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

namespace small::util {

// Explicit narrowing cast. Same as static_cast but signals intent at
// the call site: "this value is bounded by an external invariant
// (protocol field width, schema column count, ...) so the truncation
// is safe by construction."
template <typename To, typename From>
constexpr To narrow_cast(From x) noexcept {
    return static_cast<To>(x);
}

}  // namespace small::util
