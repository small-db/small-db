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
#include <unordered_map>
#include <vector>

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

#include "src/gossip/gossip.grpc.pb.h"
#include "src/gossip/gossip.pb.h"

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
        fmt::format_to(out, "{{");
        fmt::format_to(out, "value: {}, ", info.value);

        // Convert last_updated (milliseconds since epoch) to time_t
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            info.last_updated);
        std::time_t t =
            std::chrono::duration_cast<std::chrono::seconds>(ms).count();
        std::tm tm = *std::localtime(&t);

        // Format as "YYYY-MM-DD HH:MM:SS"
        char buf[32];
        std::strftime(buf, sizeof(buf), "%F %T", &tm);
        fmt::format_to(out, "last_updated: {}", buf);

        fmt::format_to(out, "}}");

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
        fmt::format_to(out, "{{");
        fmt::format_to(out, "sql_addr: {}, ", info.sql_addr);
        fmt::format_to(out, "grpc_addr: {}, ", info.grpc_addr);
        fmt::format_to(out, "data_dir: {}, ", info.data_dir);
        fmt::format_to(out, "region: {}, ", info.region);
        fmt::format_to(out, "}}");

        return out;
    }
};

}  // namespace fmt

namespace small::gossip {

inline constexpr std::string_view key_prefix_node = "node:";

GossipMessage::GossipMessage(const std::string& message) : message(message) {
    SPDLOG_ERROR("unimplemented");
}

std::vector<char> InfoStore::get_info(const std::string& key) {
    SPDLOG_ERROR("unimplemented");
    return std::vector<char>();
}

void GossipServer::update_node(
    const small::server_info::ImmutableInfo& node_info, bool sync_to_store) {
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());

    if (!this->nodes.contains(node_info.id)) {
        SPDLOG_INFO("gossip: adding new node {}", node_info);
        this->nodes.insert({node_info.id, node_info});
    }

    if (sync_to_store) {
        auto key = fmt::format("node:{}", self_info.id);

        auto entry = Entry();
        entry.set_value(nlohmann::json(self_info).dump());
        entry.set_last_update(now.count());

        this->info_store.update(key, entry);
    }
}

GossipServer::GossipServer(const small::server_info::ImmutableInfo& self_info,
                           const std::string& seed_peer)
    : self_info(self_info) {
    // add self to the nodes list
    this->update_node(self_info, true);

    std::thread([this, seed_peer]() {
        SPDLOG_INFO("gossip server started");
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(3));

            SPDLOG_INFO("gossip: communicating with peers {}", this->nodes);

            if (this->nodes.empty()) {
                if (seed_peer.empty()) {
                    SPDLOG_INFO("gossip: no peers to communicate with");
                    continue;
                }

                SPDLOG_INFO("gossip: communicating with peer {}", seed_peer);

                auto channel = grpc::CreateChannel(
                    seed_peer, grpc::InsecureChannelCredentials());
                auto stub = small::gossip::Gossip::NewStub(channel);
                grpc::ClientContext context;
                small::gossip::Entries request;
                small::gossip::Entries result;

                request = this->info_store.entries;

                grpc::Status status =
                    stub->Exchange(&context, request, &result);
                if (!status.ok()) {
                    SPDLOG_ERROR("gossip: failed to communicate with peer {} ",
                                 seed_peer);
                } else {
                    SPDLOG_INFO(
                        "gossip: successfully communicated with peer {}",
                        seed_peer);
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

std::unordered_map<std::string, small::server_info::ImmutableInfo> get_nodes() {
    return GossipServer::get_instance()->nodes;
}

small::gossip::Entries GossipServer::update(
    InfoStore& info_store, const small::gossip::Entries& peer_entries) {
    std::lock_guard<std::mutex> lock(info_store.mtx);

    small::gossip::Entries self_newer;

    // step 1: update entries that are newer in the peer
    for (const auto& [key, peer_entry] : peer_entries.entries()) {
        auto value = peer_entry.value();
        auto last_update = peer_entry.last_update();

        auto it = info_store.entries.mutable_entries()->find(key);
        if (it != info_store.entries.entries().end()) {
            if (it->second.last_update() < last_update) {
                // peer's entry is newer, update the store
                it->second.set_value(value);
                it->second.set_last_update(last_update);
            } else {
                // self's entry is newer, add it to self_newer
                self_newer.mutable_entries()->insert({key, it->second});
            }
        } else {
            // key doesn't exist in self
            info_store.entries.mutable_entries()->insert({key, peer_entry});
        }
    }

    // step 2: update entries that are newer in self
    for (const auto& [key, self_entry] : info_store.entries.entries()) {
        auto it = peer_entries.entries().find(key);
        if (it != peer_entries.entries().end()) {
            // If the key exists in both, check which one is newer
            if (self_entry.last_update() > it->second.last_update()) {
                // Add the self entry to self_newer
                self_newer.mutable_entries()->insert({key, self_entry});
            }
        } else {
            // If the key doesn't exist in the peer, add it to self_newer
            self_newer.mutable_entries()->insert({key, self_entry});
        }
    }

    // step 3: update nodes list according to the updated entries
    for (const auto& [key, self_entry] : info_store.entries.entries()) {
        if (key.starts_with(key_prefix_node)) {
            nlohmann::json j = nlohmann::json::parse(self_entry.value());
            auto node_info = j.get<small::server_info::ImmutableInfo>();
            this->update_node(node_info, false);
        }
    }

    return self_newer;
}

grpc::Status GossipServiceImpl::Exchange(grpc::ServerContext* context,
                                         const small::gossip::Entries* entries,
                                         small::gossip::Entries* response) {
    SPDLOG_INFO("gossip: received entries from peer");
    auto reply = GossipServer::get_instance()->update(
        GossipServer::get_instance()->info_store, *entries);
    *response = reply;
    return grpc::Status::OK;
}

}  // namespace small::gossip
