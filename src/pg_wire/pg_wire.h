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

#include <memory>
#include <optional>
#include <string>

// =====================================================================
// third-party libraries
// =====================================================================

#include "arrow/api.h"

namespace small::pg_wire {

void send_no_ssl_support(int sockfd);

void send_ready(int sockfd);

void send_batch(int sockfd, const std::shared_ptr<arrow::RecordBatch>& batch);

void send_empty_result(int sockfd);

void send_error(int sockfd, const std::string& error_message);

// Reads raw bytes from the socket. Returns empty string if the client
// disconnected (recv returns 0). Throws std::runtime_error on recv failure.
std::string read_bytes(int sockfd);

enum class StartupPacketType {
    SSLRequest,
    StartupMessage,
};

// Reads and classifies the initial startup packet from a PostgreSQL client.
// During the handshake phase, clients send either an SSLRequest (to negotiate
// encryption) or a StartupMessage (with protocol version and connection
// parameters). Returns nullopt if the client disconnected. Throws
// std::runtime_error on recv failure.
std::optional<StartupPacketType> read_startup_packet(int sockfd);

}  // namespace small::pg_wire
