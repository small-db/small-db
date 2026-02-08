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
// small-db libraries
// =====================================================================

#include "src/type/type.pb.h"

// =====================================================================
// self include
// =====================================================================

#include "src/type/type.h"

namespace small::type {

std::string to_string(Type type) {
    switch (type) {
        case Type::INT64:
            return "int";
        case Type::STRING:
            return "str";
        default:
            throw std::runtime_error("unknown type" +
                                     std::string(magic_enum::enum_name(type)));
    }
}

absl::StatusOr<Type> from_string(const std::string& type_name) {
    if (type_name == "int") {
        return Type::INT64;
    } else if (type_name == "str") {
        return Type::STRING;
    } else {
        return absl::InternalError("unknown type: " + type_name);
    }
}

absl::StatusOr<Type> from_ast_string(const std::string& type_name) {
    if (type_name == "int4") {
        return Type::INT64;
    } else if (type_name == "string") {
        return Type::STRING;
    } else {
        return absl::InternalError("unknown type: " + type_name);
    }
}

pqxx::oid to_pgwire_oid(Type type) {
    switch (type) {
        case Type::INT64:
            return 20;  // int8
        case Type::STRING:
            return 25;  // text
        default:
            throw std::runtime_error("unknown type" +
                                     std::string(magic_enum::enum_name(type)));
    }
}

absl::StatusOr<Type> from_pgwire_oid(pqxx::oid oid) {
    switch (oid) {
        case 20:  // int8
            return Type::INT64;
        case 25:  // text
            return Type::STRING;
        default:
            return absl::InternalError("unknown oid: " + std::to_string(oid));
    }
}

gandiva::DataTypePtr get_gandiva_type(Type type) {
    switch (type) {
        case Type::INT64:
            return arrow::int64();
        case Type::STRING:
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
        case Type::INT64:
            return 8;
        case Type::STRING:
            return -1;
        default:
            throw std::runtime_error("Unsupported type for pgwire");
    }
}

std::string encode(const Datum& datum) {
    switch (datum.value_case()) {
        case Datum::kInt64Value:
            return std::to_string(datum.int64_value());
        case Datum::kStringValue:
            return datum.string_value();
        default:
            throw std::runtime_error("unknown datum value case");
    }
}

Datum decode(const std::string& str, Type type) {
    Datum datum;
    switch (type) {
        case Type::INT64: {
            try {
                int64_t value = std::stoll(str);
                datum.set_int64_value(value);
            } catch (const std::exception& e) {
                throw std::runtime_error("failed to decode string to int64: " +
                                         str);
            }
            break;
        }
        case Type::STRING:
            datum.set_string_value(str);
            break;
        default:
            throw std::runtime_error("unsupported type for decoding");
    }
    return datum;
}

}  // namespace small::type
