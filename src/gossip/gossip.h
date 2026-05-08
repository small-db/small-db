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
// small-db libraries
// =====================================================================

#include "src/schema/schema.pb.h"
#include "src/server_info/info.h"

// =====================================================================
// small-db libraries (protobuf generated)
// =====================================================================

#include "src/gossip/gossip.grpc.pb.h"
#include "src/gossip/gossip.pb.h"

namespace small::gossip {

class GossipMessage {
   private:
    std::set<std::string> recipient_ids;
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

// One per server. Every few seconds, picks a random peer and exchanges
// (key, last-update-ts) pairs to converge the cluster's view.
class GossipServer {
   private:
    static GossipServer* instance_ptr;

    // If `seed_peer` is empty, wait for other peers to initiate contact.
    explicit GossipServer(const small::server_info::ImmutableInfo& self_info,
                          const std::string& seed_peer);

    ~GossipServer() = default;

    void add_node(const small::server_info::ImmutableInfo& node);

   public:
    void operator=(const GossipServer&) = delete;
    GossipServer(const GossipServer&) = delete;

    InfoStore store;

    small::server_info::ImmutableInfo self_info;

    std::vector<small::server_info::ImmutableInfo> get_nodes();

    static void init_instance(
        const small::server_info::ImmutableInfo& self_info,
        const std::string& seed_peer);

    static GossipServer* get_instance();

    // Merge peer_entries into the local store. Returns entries this
    // server has newer copies of (so the caller can ship them back).
    Entries update(const Entries& peer_entries);
};

// Get the nodes that satisfy the constraints. If constraints is nullopt, return
// all nodes.
//
// Return nodes in form of [node_id -> server_info].
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
