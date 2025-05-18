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
#include <utility>

// =====================================================================
// third-party libraries
// =====================================================================

// spdlog
#include "spdlog/spdlog.h"

// uuid
#include "uuid/uuid.h"

// json
#include "nlohmann/json.hpp"

// =====================================================================
// self header
// =====================================================================

#include "src/server_info/info.h"

namespace small::server_info {

// Static instance pointer definition (must be outside class)
ServerInfo* ServerInfo::instance = nullptr;

void to_json(nlohmann::json& j, const ImmutableInfo& info) {
    j = nlohmann::json{
        {"id", info.id},
        {"sql_addr", info.sql_addr},
        {"grpc_addr", info.grpc_addr},
        {"data_dir", info.data_dir},
        {"region", info.region},
        {"join", info.join},
    };
}

void from_json(const nlohmann::json& j, ImmutableInfo& info) {
    j.at("id").get_to(info.id);
    j.at("sql_addr").get_to(info.sql_addr);
    j.at("grpc_addr").get_to(info.grpc_addr);
    j.at("data_dir").get_to(info.data_dir);
    j.at("region").get_to(info.region);
    j.at("join").get_to(info.join);
}

ImmutableInfo::ImmutableInfo(const std::string& sql_addr,
                             const std::string& grpc_addr,
                             const std::string& data_dir,
                             const std::string& region, const std::string& join)
    : sql_addr(sql_addr),
      grpc_addr(grpc_addr),
      data_dir(data_dir),
      region(region),
      join(join) {
    uuid_t uuid;
    uuid_generate(uuid);
    char str[37];
    uuid_unparse(uuid, str);
    this->id = std::string(str);
}

ServerInfo::ServerInfo() {
    uuid_t uuid;
    uuid_generate(uuid);
    char str[37];
    uuid_unparse(uuid, str);
    SPDLOG_INFO("server uuid: {}", str);
    this->id = std::string(str);
}

ServerInfo::~ServerInfo() = default;

absl::Status ServerInfo::Init(const ImmutableInfo& args) {
    if (instance == nullptr) {
        instance = new ServerInfo();
        instance->db_path = args.data_dir;
        return absl::OkStatus();
    }
    SPDLOG_ERROR("ServerInfo instance is already initialized");
    return absl::InternalError("ServerInfo instance is already initialized");
}

absl::StatusOr<ServerInfo*> ServerInfo::GetInstance() {
    if (!instance) {
        return absl::InternalError("ServerInfo instance is not initialized");
    }
    return instance;
}

absl::Status init(const ImmutableInfo& args) { return ServerInfo::Init(args); }

absl::StatusOr<ServerInfo*> get_info() { return ServerInfo::GetInstance(); }

}  // namespace small::server_info
