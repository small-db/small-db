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
// third-party libraries
// =====================================================================

// arrow
#include "arrow/api.h"

// arrow gandiva
#include "gandiva/arrow.h"

// magic_enum
#include "magic_enum/magic_enum.hpp"

// =====================================================================
// self include
// =====================================================================

#include "src/type/type.h"

namespace small::type {

std::string to_string(Type type) {
    switch (type) {
        case Type::Int64:
            return "int4";
        case Type::String:
            return "string";
        default:
            throw std::runtime_error("unknown type" +
                                     std::string(magic_enum::enum_name(type)));
    }
}

absl::StatusOr<Type> from_string(const std::string& type_name) {
    if (type_name == "int4") {
        return Type::Int64;
    } else if (type_name == "string") {
        return Type::String;
    } else {
        return absl::InternalError("unknown type: " + type_name);
    }
}

pqxx::oid to_pgwire_oid(Type type) {
    switch (type) {
        case Type::Int64:
            return 20;  // int8
        case Type::String:
            return 25;  // text
        default:
            throw std::runtime_error("unknown type" +
                                     std::string(magic_enum::enum_name(type)));
    }
}

absl::StatusOr<Type> from_pgwire_oid(pqxx::oid oid) {
    switch (oid) {
        case 20:  // int8
            return Type::Int64;
        case 25:  // text
            return Type::String;
        default:
            return absl::InternalError("unknown oid: " +
                                              std::to_string(oid));
    }
}

gandiva::DataTypePtr get_gandiva_type(Type type) {
    switch (type) {
        case Type::Int64:
            return arrow::int64();
        case Type::String:
            return arrow::utf8();
        default:
            throw std::runtime_error("Unsupported type for Gandiva");
    }
}

// > For a fixed-size type, typlen is the number of bytes in the internal
// > representation of the type. But for a variable-length type, typlen is
// > negative. -1 indicates a “varlena” type (one that has a length word), -2
// > indicates a null-terminated C string.
//
// source:
// https://www.postgresql.org/docs/current/catalog-pg-type.html
int16_t get_pgwire_size(Type type) {
    switch (type) {
        case Type::Int64:
            return 8;
        case Type::String:
            return -1;
        default:
            throw std::runtime_error("Unsupported type for pgwire");
    }
}

}  // namespace small::type
