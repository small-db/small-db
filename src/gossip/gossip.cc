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

// =====================================================================
// third-party libraries
// =====================================================================

// spdlog
#include "spdlog/spdlog.h"

// =====================================================================
// local libraries
// =====================================================================

#include "src/server_info/info.h"

// =====================================================================
// self header
// =====================================================================

#include "src/gossip/gossip.h"

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
    std::thread([this, peer_addr]() {
        SPDLOG_INFO("gossip server started");
        while (true) {
            SPDLOG_INFO("gossip: communicating with peers...");

            if (this->peers.empty()) {
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

}  // namespace small::gossip
