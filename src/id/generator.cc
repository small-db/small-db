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

#include <atomic>
#include <cstdint>

// =====================================================================
// self header
// =====================================================================

#include "src/id/generator.h"

namespace id {

// Atomic counter to ensure thread-safe ID generation
std::atomic<int64_t> id_counter{0};

int64_t generate_id() {
    // Atomically increment and return the next ID
    return id_counter.fetch_add(1, std::memory_order_relaxed);
}

}  // namespace id
