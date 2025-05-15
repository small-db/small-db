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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

// absl
#include "absl/status/status.h"

// grpc
#include "grpcpp/server_builder.h"

// =====================================================================
// local libraries
// =====================================================================

#include "src/server_info/info.h"

// =====================================================================
// protobuf generated files
// =====================================================================

#include "server_registry.grpc.pb.h"
#include "server_registry.pb.h"

namespace small::server_registry {

class Peers {
   private:
    // singleton instance - protected constructor
    Peers();
    // singleton instance - protected destructor
    ~Peers();

    std::mutex mutex_;

   public:
    // singleton instance - copy blocker
    Peers(const Peers&) = delete;

    // singleton instance - assignment blocker
    void operator=(const Peers&) = delete;

    // singleton instance - get instance
    static Peers* get_instance();

    std::vector<small::server_info::ImmutableInfo> peers;

    absl::Status add(const small::server_info::ImmutableInfo& args);
};

void start_server(std::string addr);

// get servers according to the constraints, pass an empty constraints to get
// all servers
std::vector<small::server_info::ImmutableInfo> get_servers(
    std::unordered_map<std::string, std::string>& constraints);

absl::Status join(const small::server_info::ImmutableInfo& args);

class RegistryService final
    : public ::small::server_registry::ServerRegistry::Service {
   public:
    ::grpc::Status Register(::grpc::ServerContext* context,
                            const RegistryRequest* request,
                            RegistryReply* response) override;
};

}  // namespace small::server_registry
