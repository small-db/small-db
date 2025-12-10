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
    std::mutex mutex;

    Entries entries;

    // Update the entry identified by key if the new entry is newer.
    //
    // NB: This method is not thread-safe. Caller must hold the mutex before
    // calling this method.
    void update(const std::string& key, const Entry& entry);
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
    //
    // Initialize the GossipServer with self_info and seed_peer. If seed_peer
    // is empty, the GossipServer will wait other peers passively.
    explicit GossipServer(const small::server_info::ImmutableInfo& self_info,
                          const std::string& seed_peer);

    // singleton instance - protected destructor
    ~GossipServer() = default;

    // Add a node to the nodes list if not already present.
    void add_node(const small::server_info::ImmutableInfo& node);

   public:
    // singleton instance - assignment-blocker
    void operator=(const GossipServer&) = delete;

    // singleton instance - copy-blocker
    GossipServer(const GossipServer&) = delete;

    // Store of all gossip entries.
    InfoStore store;

    small::server_info::ImmutableInfo self_info;

    // Returns all known nodes in the cluster, regardless of their current
    // availability or health status.
    std::vector<small::server_info::ImmutableInfo> get_nodes();

    // singleton instance - init api
    static void init_instance(
        const small::server_info::ImmutableInfo& self_info,
        const std::string& seed_peer);

    // singleton instance - get api
    static GossipServer* get_instance();

    // Update the local store with the entries received from a peer, return
    // the entries that are newer in self.
    Entries update(const Entries& peer_entries);
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
