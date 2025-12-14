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
// c std
// =====================================================================

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>

// ====================================================================
// c++ std
// =====================================================================

#include <cstring>
#include <stdexcept>
#include <string>

// =====================================================================
// third-party libraries
// =====================================================================

#include "spdlog/spdlog.h"

// =====================================================================
// self header
// =====================================================================

#include "src/util/ip/ip.h"

namespace small::util::ip {

sockaddr_in str_to_sockaddr(const std::string& sql_addr) {
    struct sockaddr_in addr{};
    std::memset(&addr, 0, sizeof(addr));  // Zero out the structure

    // Find the position of the colon (:) separating the IP and port
    size_t colon_pos = sql_addr.find(':');
    if (colon_pos == std::string::npos) {
        throw std::invalid_argument(
            "Invalid address format. Expected ip:port.");
    }

    // Extract the port as string
    std::string port_str = sql_addr.substr(colon_pos + 1);

    // Convert the port to an integer
    int port = std::stoi(port_str);
    if (port < 1 || port > 65535) {
        throw std::out_of_range("Port number out of range (1-65535).");
    }

    addr.sin_family = AF_INET;
    // Set to accept connections from any IP address since the resolved IP is
    // local loopback (127.0.2.1) in vagrant environment.
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    return addr;
}

}  // namespace small::util::ip
