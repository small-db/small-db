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

// =====================================================================
// c++ std
// =====================================================================

#include <map>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

// json
#include "nlohmann/json.hpp"

namespace small::schema {

class NullPartition {};

class ListPartition {
   public:
    // the partition column
    std::string column_name;

    class SinglePartition {
       public:
        std::vector<std::string> values;
        std::unordered_map<std::string, std::string> constraints;
    };

    // key: partition name
    // value: partition values
    //
    // use ordered map to keep a consistent order on disk
    std::map<std::string, SinglePartition> partitions;

    std::optional<SinglePartition> lookup(std::string value);
};

using partition_t = std::variant<NullPartition, ListPartition>;

void to_json(nlohmann::json& j, const partition_t& p);

void from_json(const nlohmann::json& j, partition_t& p);

}  // namespace small::schema
