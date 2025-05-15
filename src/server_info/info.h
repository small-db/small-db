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

#include <string>

// =====================================================================
// third-party libraries
// =====================================================================

// absl
#include "absl/status/statusor.h"

namespace small::server_info {

// Immutable information of a server.
class ImmutableInfo {
   public:
    // IPv4 SQL address in the form of <ip>:<port>.
    //
    // Don't use broadcast or multicast address since this field will be used to
    // connect to the server.
    std::string sql_addr;

    // IPv4 gRPC address in the form of <ip>:<port>.
    //
    // Don't use broadcast or multicast address since this field will be used to
    // connect to the server.
    std::string grpc_addr;

    std::string data_dir;
    std::string region;
    std::string join;

    ImmutableInfo(const std::string& sql_addr, const std::string& grpc_addr,
                  const std::string& data_dir, const std::string& region,
                  const std::string& join);
};

class ServerInfo {
   private:
    // singleton instance - constructor protector
    ServerInfo();
    // singleton instance - destructor protector
    ~ServerInfo();

    static ServerInfo* instance;

   public:
    // singleton instance - copy blocker
    ServerInfo(const ServerInfo&) = delete;

    // singleton instance - assignment blocker
    void operator=(const ServerInfo&) = delete;

    std::string db_path;

    std::string id;

    static absl::Status Init(const ImmutableInfo& args);

    // singleton instance - get instance
    static absl::StatusOr<ServerInfo*> GetInstance();
};

absl::Status init(const ImmutableInfo& args);

absl::StatusOr<ServerInfo*> get_info();

}  // namespace small::server_info
