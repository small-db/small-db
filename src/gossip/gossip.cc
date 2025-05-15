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

GossipServer* GossipServer::instance_ptr = nullptr;

void GossipServer::init_instance(
    const small::server_info::ImmutableInfo& self_info) {
    if (instance_ptr == nullptr) {
        instance_ptr = new GossipServer(self_info);
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

}  // namespace small::gossip
