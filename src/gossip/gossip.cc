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

#include <format>
#include <string>

// =====================================================================
// third-party libraries
// =====================================================================

// spdlog
#include "spdlog/spdlog.h"

// grpc
#include "grpcpp/create_channel.h"

// =====================================================================
// local libraries
// =====================================================================

#include "src/server_info/info.h"

// =====================================================================
// protobuf generated files
// =====================================================================

#include "gossip.grpc.pb.h"
#include "gossip.pb.h"

// =====================================================================
// self header
// =====================================================================

#include "src/gossip/gossip.h"

namespace fmt {

template <typename K, typename V>
struct formatter<std::unordered_map<K, V>> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template <typename Context>
    constexpr auto format(const std::unordered_map<K, V>& map,
                          Context& ctx) const {
        auto out = ctx.out();
        fmt::format_to(out, "{{");
        bool first = true;
        for (const auto& [k, v] : map) {
            if (!first) fmt::format_to(out, ", ");
            fmt::format_to(out, "{}: {}", k, v);
            first = false;
        }
        return fmt::format_to(out, "}}");
    }
};

template <typename T>
struct formatter<small::gossip::Info<T>> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template <typename Context>
    constexpr auto format(const small::gossip::Info<T>& info,
                          Context& ctx) const {
        auto out = ctx.out();
        return out;
    }
};

template <>
struct formatter<small::server_info::ImmutableInfo> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template <typename Context>
    constexpr auto format(const small::server_info::ImmutableInfo& info,
                          Context& ctx) const {
        auto out = ctx.out();
        return out;
    }
};

}  // namespace fmt

namespace small::gossip {

GossipMessage::GossipMessage(const std::string& message) : message(message) {
    SPDLOG_ERROR("unimplemented");
}

std::vector<char> InfoStore::get_info(const std::string& key) {
    SPDLOG_ERROR("unimplemented");
    return std::vector<char>();
}

GossipServer::GossipServer(const small::server_info::ImmutableInfo& self_info,
                           const std::string& peer_addr)
    : self_info(self_info) {
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    auto key = fmt::format("node:{}", self_info.id);
    this->peers.emplace(
        key, Info<small::server_info::ImmutableInfo>(self_info, now));
    SPDLOG_INFO("peers: {}", this->peers);

    std::thread([this, peer_addr]() {
        SPDLOG_INFO("gossip server started");
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(3));

            SPDLOG_INFO("gossip: communicating with peers...");

            if (this->peers.empty()) {
                SPDLOG_INFO("gossip: communicating with peer {}", peer_addr);

                auto channel = grpc::CreateChannel(
                    peer_addr, grpc::InsecureChannelCredentials());
                auto stub = small::gossip::Gossip::NewStub(channel);
                grpc::ClientContext context;
                small::gossip::Entries request;
                small::gossip::Entries result;
                grpc::Status status =
                    stub->Exchange(&context, request, &result);
                if (!status.ok()) {
                    SPDLOG_ERROR("gossip: failed to communicate with peer {}",
                                 peer_addr);
                } else {
                    SPDLOG_INFO(
                        "gossip: successfully communicated with peer {}",
                        peer_addr);
                }
            } else {
            }
        }
    }).detach();
}

GossipServer* GossipServer::instance_ptr = nullptr;

void GossipServer::init_instance(
    const small::server_info::ImmutableInfo& self_info,
    const std::string& peer_addr) {
    if (instance_ptr == nullptr) {
        instance_ptr = new GossipServer(self_info, peer_addr);
    } else {
        SPDLOG_ERROR("gossip server instance already initialized");
    }
}

GossipServer* GossipServer::get_instance() {
    if (instance_ptr == nullptr) {
        SPDLOG_ERROR("gossip instance not initialized");
        return nullptr;
    }
    return instance_ptr;
}

void GossipServer::transmit_message(const GossipMessage& message) {}

void GossipServer::broadcast_message(const std::string& message) {
    GossipMessage gossip_message(message);
    transmit_message(gossip_message);
}

std::vector<small::server_info::ImmutableInfo> get_nodes() {
    auto nodes_bytes =
        GossipServer::get_instance()->info_store.get_info("nodes");

    return std::vector<small::server_info::ImmutableInfo>();
}

grpc::Status GossipService::Exchange(grpc::ServerContext* context,
                                     const small::gossip::Entries* entries,
                                     small::gossip::Entries* response) {
    SPDLOG_INFO("gossip: received entries from peer");
    return grpc::Status::OK;
}

}  // namespace small::gossip

// template <typename K, typename V>
// struct std::formatter<std::unordered_map<K, V>> {
//     constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

//     template <typename FormatContext>
//     auto format(const std::unordered_map<K, V>& map, FormatContext& ctx) {
//         auto out = ctx.out();
//         std::format_to(out, "{{");
//         bool first = true;
//         for (const auto& [k, v] : map) {
//             if (!first) std::format_to(out, ", ");
//             std::format_to(out, "{}: {}", k, v);
//             first = false;
//         }
//         return std::format_to(out, "}}");
//     }
// };

// template <>
// struct std::formatter<
//     std::unordered_map<std::string,
//                        small::gossip::Info<small::server_info::ImmutableInfo>>,
//     char> {
//     bool quoted = false;

//     template <class ParseContext>
//     constexpr ParseContext::iterator parse(ParseContext& ctx) {
//         return ctx.begin();
//     }

//     template <class FmtContext>
//     FmtContext::iterator format(
//         std::unordered_map<std::string, small::gossip::Info<
//                                             small::server_info::ImmutableInfo>>,
//         FmtContext& ctx) const {
//         std::ostringstream out;
//         std::string result = out.str();
//         return std::ranges::copy(std::move(result), ctx.out()).out;
//     }
// };