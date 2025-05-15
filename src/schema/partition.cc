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
// self header
// =====================================================================

#include "src/schema/partition.h"

namespace small::schema {

void to_json(nlohmann::json& j, const ListPartition::SinglePartition& p) {
    j = nlohmann::json{
        {"values", p.values},
        {"constraints", p.constraints},
    };
}

void from_json(const nlohmann::json& j, ListPartition::SinglePartition& p) {
    j.at("values").get_to(p.values);
    j.at("constraints").get_to(p.constraints);
}

void to_json(nlohmann::json& j, const partition_t& p) {
    std::visit(
        [&j](const auto& partition) {
            using T = std::decay_t<decltype(partition)>;
            if constexpr (std::is_same_v<T, ListPartition>) {
                j["type"] = "ListPartition";
                j["content"] = nlohmann::json{
                    {"column_name", partition.column_name},
                    {"partitions", partition.partitions},
                };
            } else {
                j["type"] = "NullPartition";
                j["content"] = nullptr;
            }
        },
        p);
}

void from_json(const nlohmann::json& j, partition_t& p) {
    std::string type = j.at("type").get<std::string>();

    if (type == "ListPartition") {
        ListPartition partition;
        const auto& content = j.at("content");

        content.at("column_name").get_to(partition.column_name);
        content.at("partitions").get_to(partition.partitions);

        p = partition;
    } else if (type == "NullPartition") {
        p = NullPartition{};
    } else {
        throw std::runtime_error("Unknown partition type in from_json: " +
                                 type);
    }
}

std::optional<ListPartition::SinglePartition> ListPartition::lookup(
    std::string value) {
    for (auto& [name, partition] : partitions) {
        if (std::find(partition.values.begin(), partition.values.end(),
                      value) != partition.values.end()) {
            return partition;
        }
    }
    return std::nullopt;
}

}  // namespace small::schema
