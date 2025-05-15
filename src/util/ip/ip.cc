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
#include <netinet/in.h>
#include <sys/socket.h>

// ====================================================================
// c++ std
// =====================================================================

#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

// =====================================================================
// self header
// =====================================================================

#include "src/util/ip/ip.h"

namespace small::util::ip {

sockaddr_in str_to_sockaddr(const std::string& sql_addr) {
    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));  // Zero out the structure

    // Find the position of the colon (:) separating the IP and port
    size_t colon_pos = sql_addr.find(':');
    if (colon_pos == std::string::npos) {
        throw std::invalid_argument(
            "Invalid address format. Expected ip:port.");
    }

    // Extract the IP and port as strings
    std::string ip = sql_addr.substr(0, colon_pos);
    std::string port_str = sql_addr.substr(colon_pos + 1);

    // Convert the port to an integer
    int port = std::stoi(port_str);
    if (port < 1 || port > 65535) {
        throw std::out_of_range("Port number out of range (1-65535).");
    }

    // Fill the sockaddr_in structure
    addr.sin_family = AF_INET;    // IPv4
    addr.sin_port = htons(port);  // Convert port to network byte order

    // Convert the IP address to binary form
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
        throw std::invalid_argument("Invalid IP address format.");
    }

    return addr;
}

}  // namespace small::util::ip
