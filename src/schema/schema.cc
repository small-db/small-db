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

// json
#include "nlohmann/json.hpp"

// =====================================================================
// local libraries
// =====================================================================

#include "src/type/type.h"

// =====================================================================
// generated files
// =====================================================================

#include "src/schema/schema.grpc.pb.h"
#include "src/schema/schema.pb.h"

// =====================================================================
// self header
// =====================================================================

#include "src/schema/schema.h"

namespace small::schema {

void to_json(nlohmann::json& j, const Column& c) {
    j = nlohmann::json{
        {"name", c.name()},
        {"type", c.type()},
        {"is_primary_key", c.is_primary_key()},
    };
}

void from_json(const nlohmann::json& j, Column& c) {
    c.set_name(j.at("name").get<std::string>());
    c.set_type(j.at("type").get<small::type::Type>());
    c.set_is_primary_key(j.at("is_primary_key").get<bool>());
}

int get_pk_index(const Table& table) {
    for (int i = 0; i < table.columns().size(); i++) {
        if (table.columns()[i].is_primary_key()) {
            return i;
        }
    }
    throw std::runtime_error("no primary key found in table " + table.name());
}

// void to_json(nlohmann::json& j, const Table& t) {
//     j = nlohmann::json{{"name", t.name}, {"columns", t.columns}};
// }

// void from_json(const nlohmann::json& j, Table& t) {
//     j.at("name").get_to(t.name);
//     j.at("columns").get_to(t.columns);
// }

// Column::Column(const std::string& name, const small::type::Type& type,
//                bool is_primary_key)
//     : name(name), type(type), is_primary_key(is_primary_key) {}

// void Column::set_primary_key(bool set) { is_primary_key = set; }

// Table::Table(const std::string& name, const std::vector<Column>& columns)
//     : name(name), columns(columns) {}

// int Table::get_pk_index() {
//     for (int i = 0; i < columns.size(); ++i) {
//         if (columns[i].is_primary_key) {
//             return i;
//         }
//     }
//     return -1;
// }

}  // namespace small::schema
