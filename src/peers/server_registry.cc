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

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

// grpc
#include "grpc/grpc.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/server_builder.h"

// spdlog
#include "spdlog/spdlog.h"

// =====================================================================
// local libraries
// =====================================================================

#include "src/server_info/info.h"
#include "src/util/ip/ip.h"

// =====================================================================
// protobuf generated files
// =====================================================================

#include "server_registry.grpc.pb.h"
#include "server_registry.pb.h"

// =====================================================================
// self header
// =====================================================================

#include "src/peers/server_registry.h"

namespace small::server_registry {

absl::Status Peers::add(
    const small::server_info::ImmutableInfo& args) {
    std::lock_guard<std::mutex> lock(this->mutex_);

    SPDLOG_INFO(
        "[server status] register server: sql_address: {}, rpc_address: {}, "
        "region: "
        "{}",
        args.sql_addr, args.grpc_addr, args.region);
    this->peers.push_back(args);
    return absl::OkStatus();
}

Peers::Peers() = default;
Peers::~Peers() = default;

Peers* Peers::get_instance() {
    static Peers instance;
    return &instance;
}

grpc::Status RegistryService::Register(grpc::ServerContext* context,
                                       const RegistryRequest* request,
                                       RegistryReply* response) {
    SPDLOG_INFO(
        "[server] register server: sql_address: {}, rpc_address: {}, "
        "region: {}",
        request->sql_address(), request->rpc_address(), request->region());

    auto status =
        small::server_registry::Peers::get_instance()->add(
            small::server_info::ImmutableInfo(request->sql_address(),
                                           request->rpc_address(), "",
                                           request->region(), ""));

    if (!status.ok()) {
        SPDLOG_ERROR("failed to register server: {}", status.ToString());
        response->set_success(false);
        return grpc::Status(grpc::StatusCode::INTERNAL, status.ToString());
    }

    response->set_success(true);

    return grpc::Status::OK;
}

std::vector<small::server_info::ImmutableInfo> get_servers(
    std::unordered_map<std::string, std::string>& constraints) {
    std::vector<small::server_info::ImmutableInfo> result;
    auto servers =
        small::server_registry::Peers::get_instance()->peers;
    SPDLOG_INFO("get servers: {}", servers.size());
    for (const auto& server : servers) {
        SPDLOG_INFO("server: sql_address: {}, rpc_address: {}, region: {}",
                    server.sql_addr, server.grpc_addr, server.region);
        SPDLOG_INFO("constraints: ");
        for (const auto& [k, v] : constraints) {
            SPDLOG_INFO("key: {}, value: {}", k, v);
        }
        bool match = true;
        for (const auto& [k, v] : constraints) {
            if (k == "sql_address" && server.sql_addr != v) {
                match = false;
                break;
            } else if (k == "rpc_address" && server.grpc_addr != v) {
                match = false;
                break;
            } else if (k == "region" && server.region != v) {
                match = false;
                break;
            }
        }
        if (match) {
            result.push_back(server);
        }
    }
    return result;
}

void start_server(std::string addr) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());

    auto service = std::make_shared<small::server_registry::RegistryService>();
    builder.RegisterService(service.get());

    auto server = builder.BuildAndStart();
    std::thread([server = std::move(server), service, addr]() mutable {
        SPDLOG_INFO("server started, address: {}", addr);
        server->Wait();
        SPDLOG_INFO("server stopped, address: {}", addr);
    }).detach();
}

absl::Status join(const small::server_info::ImmutableInfo& args) {
    std::string peer_addr = args.join;
    if (peer_addr.empty()) {
        auto addr = small::util::ip::str_to_sockaddr(args.grpc_addr);
        peer_addr = fmt::format("127.0.0.1:{}", ntohs(addr.sin_port));
    }

    SPDLOG_INFO("join peer addr: {}", peer_addr);

    small::server_registry::RegistryRequest request;
    request.set_sql_address(args.sql_addr);
    request.set_rpc_address(args.grpc_addr);
    request.set_region(args.region);
    SPDLOG_INFO(
        "[client] register server: sql_address: {}, rpc_address: {}, region: "
        "{}",
        request.sql_address(), request.rpc_address(), request.region());

    auto channel =
        grpc::CreateChannel(peer_addr, grpc::InsecureChannelCredentials());
    std::unique_ptr<small::server_registry::ServerRegistry::Stub> stub =
        small::server_registry::ServerRegistry::NewStub(channel);
    small::server_registry::RegistryReply result;

    const int max_retries = 5;
    for (int attempt = 1; attempt <= max_retries; ++attempt) {
        grpc::ClientContext context;
        grpc::Status status = stub->Register(&context, request, &result);
        if (!status.ok() && attempt < max_retries) {
            SPDLOG_INFO("failed to join peer: {}, retrying...", peer_addr);
            std::this_thread::sleep_for(std::chrono::seconds(1));
        } else {
            break;
        }
    }
    SPDLOG_INFO("joined peer: {}, result: {}", peer_addr, result.success());
    return absl::OkStatus();
}

}  // namespace small::server_registry
