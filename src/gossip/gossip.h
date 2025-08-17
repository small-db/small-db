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

#include <set>
#include <string>
#include <unordered_map>
#include <vector>

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

namespace small::gossip {

class GossipMessage {
   private:
    // servers that already received the message
    std::set<std::string> recipient_ids;

    // message content
    std::string message;

   public:
    explicit GossipMessage(const std::string& message);

    void add_recipient(const std::string& recipient_id);
};

class InfoStore {
   public:
    std::mutex mtx;

    // std::vector<char> get_info(const std::string& key);
    Entries entries;

    std::vector<char> get_info(const std::string& key);

    void update(const std::string& key, const Entry& entry) {
        std::lock_guard<std::mutex> lock(mtx);

        auto it = entries.entries().find(key);
        if (it != entries.entries().end() &&
            it->second.last_update() >= entry.last_update()) {
            // the stored entry is newer, do not update
            return;
        }

        entries.mutable_entries()->insert({key, entry});
    }
};

template <typename T>
class Info {
   public:
    T value;
    std::chrono::milliseconds last_updated;

    Info(const T& val, std::chrono::milliseconds ts)
        : value(val), last_updated(ts) {}

    bool is_newer_than(const Info<T>& other) const {
        return last_updated > other.last_updated;
    }

    void update(const T& new_val, std::chrono::milliseconds ts) {
        if (ts >= last_updated) {
            value = new_val;
            last_updated = ts;
        }
    }
};

// Design:
// - Each server own exactly one GossipServer instance.
// - Every 3 seconds, the GossipServer randomly selects several peers to
//   talk with.
// - Each talk consists of 3 steps:
//   1. Send the list of keys and the "latest-update" timestamp of each key to
//      the peer.
//   2. The peer responds with the keys that it has a version of as well as the
//      "latest-update" timestamp and the value of each key.
//   3. GossipServer updates its local store with the values received from the
//      peer.
//
// Notes:
// - At least one peer is needed to initiate the GossipServer. We don't use
//   multicast and broadcast since they can only be used in LAN, and small-db
//   are supposted to be use in WAN and LAN.
class GossipServer {
   private:
    // singleton instance - the only instance
    static GossipServer* instance_ptr;

    // singleton instance - protected constructor
    explicit GossipServer(const small::server_info::ImmutableInfo& self_info,
                          const std::string& peer_addr);

    // singleton instance - protected destructor
    ~GossipServer() = default;

    void transmit_message(const GossipMessage& message);

    small::server_info::ImmutableInfo self_info;

    void update_node(const small::server_info::ImmutableInfo& node_info,
                     bool sync_to_store);

   public:
    // singleton instance - assignment-blocker
    void operator=(const GossipServer&) = delete;

    // singleton instance - copy-blocker
    GossipServer(const GossipServer&) = delete;

    InfoStore info_store;

    std::unordered_map<std::string, small::server_info::ImmutableInfo> nodes;

    // singleton instance - init api
    static void init_instance(
        const small::server_info::ImmutableInfo& self_info,
        const std::string& seed_peer);

    // singleton instance - get api
    static GossipServer* get_instance();

    void broadcast_message(const std::string& message);

    Entries update(InfoStore& info_store, const Entries& peer_entries);
};

std::unordered_map<std::string, small::server_info::ImmutableInfo> get_nodes(
    const std::optional<google::protobuf::Map<std::string, std::string>>&
        constraints);

class GossipServiceImpl final : public small::gossip::Gossip::Service {
   public:
    grpc::Status Exchange(grpc::ServerContext* context,
                          const small::gossip::Entries* entries,
                          small::gossip::Entries* response) final;
};

}  // namespace small::gossip
