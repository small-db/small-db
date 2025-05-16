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

// =====================================================================
// local libraries
// =====================================================================

#include "src/server_info/info.h"

// =====================================================================
// protobuf generated files
// =====================================================================

// #include "gossip.grpc.pb.h"
// #include "gossip.pb.h"

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
    std::vector<char> get_info(const std::string& key);
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

   public:
    // singleton instance - assignment-blocker
    void operator=(const GossipServer&) = delete;

    // singleton instance - copy-blocker
    GossipServer(const GossipServer&) = delete;

    InfoStore info_store;

    std::vector<small::server_info::ImmutableInfo> peers;

    // singleton instance - init api
    static void init_instance(
        const small::server_info::ImmutableInfo& self_info,
        const std::string& peer_addr);

    // singleton instance - get api
    static GossipServer* get_instance();

    void broadcast_message(const std::string& message);
};

std::vector<small::server_info::ImmutableInfo> get_nodes();

// class GossipService final : public small::gossip::Gossip::Service {
//    public:
//     explicit GossipService() = default;

//     grpc::Status Gossip(grpc::ServerContext* context,
//                         const small::gossip::GossipRequest* request,
//                         small::gossip::GossipResponse* response) override;
// };

}  // namespace small::gossip
