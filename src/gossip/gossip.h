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

class GossipServer {
   private:
    // singleton instance - the only instance
    static GossipServer* instance_ptr;

    // singleton instance - protected constructor
    GossipServer() = default;

    // singleton instance - protected destructor
    ~GossipServer() = default;

    void transmit_message(const GossipMessage& message);

    small::server_info::ImmutableInfo self_info;

   public:
    // singleton instance - assignment-blocker
    void operator=(const GossipServer&) = delete;

    // singleton instance - copy-blocker
    GossipServer(const GossipServer&) = delete;

    // singleton instance - init api
    static void init_instance(
        const small::server_info::ImmutableInfo& self_info);

    // singleton instance - get api
    static GossipServer* get_instance();

    explicit GossipServer(const small::server_info::ImmutableInfo& self_info)
        : self_info(self_info) {}

    void broadcast_message(const std::string& message);
};

}  // namespace small::gossip
