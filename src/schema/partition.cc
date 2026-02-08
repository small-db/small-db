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

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/schema/schema.pb.h"

// =====================================================================
// self header
// =====================================================================

#include "src/schema/partition.h"

namespace small::schema {

std::optional<small::schema::ListPartitionItem> lookup(
    const small::schema::ListPartition& list_partition,
    const std::string& value) {
    auto partitions = list_partition.partitions();
    for (auto& entry : partitions) {
        auto& partition = entry.second;
        for (const auto& v : partition.values()) {
            if (v == value) {
                return partition;
            }
        }
    }
    return std::nullopt;
}

}  // namespace small::schema
