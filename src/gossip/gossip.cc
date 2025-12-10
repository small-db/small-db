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

#include <random>
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

#include "src/schema/schema.pb.h"
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

inline constexpr std::string_view KEY_PREFIX_NODE = "node:";

void InfoStore::update(const std::string& key, const Entry& entry) {
    auto it = entries.entries().find(key);
    if (it != entries.entries().end() &&
        it->second.last_update() >= entry.last_update()) {
        // the stored entry is newer, do not update
        return;
    }

    entries.mutable_entries()->insert({key, entry});
}

void GossipServer::add_node(const small::server_info::ImmutableInfo& node) {
    SPDLOG_INFO("gossip: adding node {}", node);
    std::lock_guard<std::mutex> lock(this->store.mutex);

    auto key = fmt::format("node:{}", node.id);
    if (this->store.entries.entries().contains(key)) {
        return;
    }

    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    auto entry = Entry();
    entry.set_value(nlohmann::json(node).dump());
    entry.set_last_update(now.count());

    this->store.update(key, entry);
}

std::vector<small::server_info::ImmutableInfo> GossipServer::get_nodes() {
    // std::lock_guard<std::mutex> lock(this->store.mutex);

    std::unique_lock<std::mutex> lock(this->store.mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        SPDLOG_WARN(
            "gossip: get_nodes failed to acquire lock, returning empty "
            "nodes list");
        return {};
    }

    std::vector<small::server_info::ImmutableInfo> nodes;

    for (const auto& [key, entry] : this->store.entries.entries()) {
        if (key.starts_with(KEY_PREFIX_NODE)) {
            nlohmann::json j = nlohmann::json::parse(entry.value());
            auto node_info = j.get<small::server_info::ImmutableInfo>();
            nodes.push_back(node_info);
        }
    }

    return nodes;
}

GossipServer::GossipServer(const small::server_info::ImmutableInfo& self_info,
                           const std::string& seed_peer)
    : self_info(self_info) {
    // add self to the nodes list
    this->add_node(self_info);

    std::thread([this, seed_peer]() {
        while (true) {
            SPDLOG_INFO("gossip: starting a new round");

            std::this_thread::sleep_for(std::chrono::seconds(3));

            // Select the peer to communicate with in this round.
            auto nodes = this->get_nodes();
            std::string peer_addr;

            if (!seed_peer.empty()) {
                // Use seed peer if present
                peer_addr = seed_peer;
            } else if (nodes.size() > 1) {
                // Choose a random node from nodes list (not self)
                std::vector<small::server_info::ImmutableInfo> other_nodes;
                for (const auto& node : nodes) {
                    if (node.id != this->self_info.id) {
                        other_nodes.push_back(node);
                    }
                }

                if (!other_nodes.empty()) {
                    std::random_device rd;
                    std::mt19937 gen(rd());
                    std::uniform_int_distribution<> dis(
                        0, static_cast<int>(other_nodes.size() - 1));
                    peer_addr = other_nodes[dis(gen)].grpc_addr;
                }
            }

            SPDLOG_INFO("gossip: selected peer {}", peer_addr);

            if (peer_addr.empty()) {
                // No peer available, wait passively
                continue;
            }

            auto channel = grpc::CreateChannel(
                peer_addr, grpc::InsecureChannelCredentials());
            auto stub = small::gossip::Gossip::NewStub(channel);
            grpc::ClientContext context;
            small::gossip::Entries request;
            small::gossip::Entries result;

            request = this->store.entries;

            grpc::Status status = stub->Exchange(&context, request, &result);
            if (status.ok()) {
                auto newer_entries = this->update(result);
            } else {
                SPDLOG_ERROR("gossip: failed to communicate with peer {} ",
                             peer_addr);
            }
        }
    }).detach();
}

GossipServer* GossipServer::instance_ptr = nullptr;

void GossipServer::init_instance(
    const small::server_info::ImmutableInfo& self_info,
    const std::string& seed_peer) {
    if (instance_ptr == nullptr) {
        instance_ptr = new GossipServer(self_info, seed_peer);
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

std::unordered_map<std::string, small::server_info::ImmutableInfo> get_nodes(
    const std::optional<google::protobuf::Map<std::string, std::string>>&
        constraints) {
    auto nodes_vec = GossipServer::get_instance()->get_nodes();

    std::unordered_map<std::string, small::server_info::ImmutableInfo> nodes;
    for (const auto& node : nodes_vec) {
        nodes[node.id] = node;
    }

    // TODO: Remove hard-coded filtering logic.
    if (constraints && constraints->contains("region")) {
        const std::string& required_region = constraints->at("region");
        std::erase_if(nodes, [&](auto& kv) {
            return kv.second.region != required_region;
        });
    }
    return nodes;
}

small::gossip::Entries GossipServer::update(
    const small::gossip::Entries& peer_entries) {
    std::lock_guard<std::mutex> lock(this->store.mutex);

    small::gossip::Entries self_newer;

    // step 1: update entries that are newer in the peer
    for (const auto& [key, peer_entry] : peer_entries.entries()) {
        auto value = peer_entry.value();
        auto last_update = peer_entry.last_update();

        auto it = this->store.entries.mutable_entries()->find(key);
        if (it != this->store.entries.entries().end()) {
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
            this->store.entries.mutable_entries()->insert({key, peer_entry});
        }
    }

    // step 2: update entries that are newer in self
    for (const auto& [key, self_entry] : this->store.entries.entries()) {
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

    return self_newer;
}

grpc::Status GossipServiceImpl::Exchange(grpc::ServerContext* context,
                                         const small::gossip::Entries* entries,
                                         small::gossip::Entries* response) {
    auto reply = GossipServer::get_instance()->update(*entries);
    *response = reply;
    return grpc::Status::OK;
}

}  // namespace small::gossip
