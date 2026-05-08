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
#include <grpcpp/server_builder.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

// =====================================================================
// c++ std
// =====================================================================

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

// pg_query
#include "pg_query.h"
#include "pg_query.pb-c.h"

// spdlog
#include "spdlog/fmt/bin_to_hex.h"
#include "spdlog/spdlog.h"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/catalog/catalog.h"
#include "src/execution/insert.h"
#include "src/execution/query.h"
#include "src/execution/update.h"
#include "src/gossip/gossip.h"
#include "src/pg_wire/pg_wire.h"
#include "src/server_info/info.h"
#include "src/txn/handle.h"
#include "src/txn/txn.h"
#include "src/util/ip/ip.h"
#include "src/util/narrow/narrow.h"

// =====================================================================
// self header
// =====================================================================

#include "src/server/server.h"

#define BACKLOG 512
#define MAX_EVENTS 128
#define MAX_MESSAGE_LEN 2048

namespace small::server {

std::atomic<bool> stopSignal = false;

int32_t read_int32_chars(char* buffer) {
    int32_t network_value;
    memcpy(&network_value, buffer, sizeof(network_value));
    auto value = small::util::narrow_cast<int32_t>(ntohl(network_value));
    return value;
}

class SocketsManager {
   public:
    enum class SocketState {
        StartUp,
        NoSSLAcknowledged,
        ReadyForQuery,
    };

    static std::string format(SocketState state) {
        switch (state) {
            case SocketState::StartUp:
                return "StartUp";
            case SocketState::NoSSLAcknowledged:
                return "NoSSLAcknowledged";
            default:
                return "Unknown";
        }
    }

   private:
    std::unordered_map<int, SocketState> socket_states;
    std::unordered_map<int, small::txn::Txn> txn_states;

    static SocketsManager* instancePtr;
    static std::mutex mtx;

    SocketsManager() = default;

   public:
    void operator=(const SocketsManager&) = delete;
    SocketsManager(const SocketsManager& obj) = delete;

    static SocketsManager* getInstance() {
        if (instancePtr == nullptr) {
            std::lock_guard<std::mutex> lock(mtx);
            if (instancePtr == nullptr) {
                instancePtr = new SocketsManager();
            }
        }
        return instancePtr;
    }

    static SocketState get_socket_state(int sockfd) {
        auto instance = getInstance();

        auto it = instance->socket_states.find(sockfd);
        if (it == instance->socket_states.end()) {
            instance->socket_states[sockfd] = SocketState::StartUp;
            return SocketState::StartUp;
        }

        return it->second;
    }

    static void set_socket_state(int sockfd, SocketState state) {
        auto instance = getInstance();
        instance->socket_states[sockfd] = state;
    }

    static void remove_socket_state(int sockfd) {
        auto instance = getInstance();
        instance->socket_states.erase(sockfd);
        instance->txn_states.erase(sockfd);
    }

    // Returns a reference to the per-connection transaction state. The
    // entry is default-constructed (inactive, no buffered writes) on
    // first access, so `auto& txn = get_txn_state(fd)` is safe even if
    // the connection has never run BEGIN before.
    static small::txn::Txn& get_txn_state(int sockfd) {
        auto instance = getInstance();
        return instance->txn_states[sockfd];
    }
};

SocketsManager* SocketsManager::instancePtr = nullptr;
std::mutex SocketsManager::mtx;

void handle_command(std::string& command, int sockfd) {
    SPDLOG_INFO("command: {}", command);

    PgQueryParseResult result;

    result = pg_query_parse(command.c_str());
    if (result.error != nullptr) {
        SPDLOG_ERROR("error parsing query: {}", result.error->message);
        small::pg_wire::send_error(sockfd, result.error->message);
        return;
    }
    SPDLOG_INFO("ast: {}", result.parse_tree);
    pg_query_free_parse_result(result);

    PgQueryProtobufParseResult pgquery_pbparse_result =
        pg_query_parse_protobuf_opts(command.c_str(), PG_QUERY_PARSE_DEFAULT);

    auto unpacked = pg_query__parse_result__unpack(
        nullptr, pgquery_pbparse_result.parse_tree.len,
        (const uint8_t*)pgquery_pbparse_result.parse_tree.data);

    auto node_case = unpacked->stmts[0]->stmt->node_case;

    auto& txn = SocketsManager::get_txn_state(sockfd);
    for (int i = 0; i < unpacked->n_stmts; i++) {
        auto result = txn.ExecuteNode(unpacked->stmts[i]->stmt);
        if (!result.ok()) {
            SPDLOG_ERROR("error handling statement: {}",
                         result.status().ToString());
            small::pg_wire::send_error(sockfd, result.status().ToString());
            return;
        }

        auto record_batch = result.value();

        if (record_batch->num_rows() == 0) {
            small::pg_wire::send_empty_result(sockfd);
            return;
        } else {
            SPDLOG_INFO("result batch: {}", record_batch->ToString());
            small::pg_wire::send_batch(sockfd, record_batch);
            return;
        }
    }
}

// Handles a single client event. Returns false if the connection should be
// closed (client disconnected, sent terminate, or sent invalid data).
bool handle_client_event(int sockfd) {
    auto state = SocketsManager::get_socket_state(sockfd);
    switch (state) {
        case SocketsManager::SocketState::StartUp: {
            auto packet_type =
                small::pg_wire::read_startup_packet(sockfd);
            if (!packet_type.has_value()) return false;

            switch (packet_type.value()) {
                case small::pg_wire::StartupPacketType::SSLRequest:
                    small::pg_wire::send_no_ssl_support(sockfd);
                    SocketsManager::set_socket_state(
                        sockfd,
                        SocketsManager::SocketState::NoSSLAcknowledged);
                    return true;
                case small::pg_wire::StartupPacketType::StartupMessage:
                    small::pg_wire::send_ready(sockfd);
                    SocketsManager::set_socket_state(
                        sockfd,
                        SocketsManager::SocketState::ReadyForQuery);
                    return true;
                default:
                    SPDLOG_ERROR("unknown startup packet type: {}",
                                 static_cast<int>(packet_type.value()));
                    return false;
            }
        }

        case SocketsManager::SocketState::NoSSLAcknowledged: {
            auto packet_type =
                small::pg_wire::read_startup_packet(sockfd);
            if (!packet_type.has_value()) return false;

            switch (packet_type.value()) {
                case small::pg_wire::StartupPacketType::StartupMessage:
                    small::pg_wire::send_ready(sockfd);
                    SocketsManager::set_socket_state(
                        sockfd,
                        SocketsManager::SocketState::ReadyForQuery);
                    return true;
                default:
                    SPDLOG_ERROR("unknown startup packet type: {}",
                                 static_cast<int>(packet_type.value()));
                    return false;
            }
        }

        case SocketsManager::SocketState::ReadyForQuery: {
            std::string message = small::pg_wire::read_bytes(sockfd);
            if (message.empty()) return false;

            switch (message[0]) {
                case 'Q': {
                    int32_t query_len =
                        read_int32_chars(message.data() + 1);
                    std::string command(message.data() + 5, query_len - 4);
                    handle_command(command, sockfd);
                    return true;
                }
                case 'X':
                    return false;
                default:
                    SPDLOG_ERROR("unknown message type: {}", message[0]);
                    return false;
            }
        }

        default:
            SPDLOG_ERROR("unknown socket state: {}",
                         SocketsManager::format(state));
            return false;
    }
}

void start_grpc_server(
    const std::string& addr,
    const std::vector<std::shared_ptr<grpc::Service>>& services) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());

    for (const auto& service : services) {
        builder.RegisterService(service.get());
    }

    auto server = builder.BuildAndStart();
    std::thread([server = std::move(server), addr, services]() mutable {
        SPDLOG_INFO("grpc server started, listening address: {}", addr);
        server->Wait();
        SPDLOG_INFO("grpc server stopped, listening address: {}", addr);
    }).detach();
}

int RunServer(const small::server_info::ImmutableInfo& args) {
    auto status = small::server_info::init(args);
    if (!status.ok()) {
        SPDLOG_ERROR("failed to init server: {}", status.ToString());
        return EXIT_FAILURE;
    }

    small::catalog::CatalogManager::InitInstance();
    small::gossip::GossipServer::init_instance(args, args.join);

    SPDLOG_INFO(
        "start server: sql_address: {}, grpc_address: {}, region: {}"
        " data_dir: {}",
        args.sql_addr, args.grpc_addr, args.region, args.data_dir);

    start_grpc_server(
        args.grpc_addr,
        {
            std::make_shared<small::execution::InsertServiceImpl>(),
            std::make_shared<small::execution::UpdateServiceImpl>(),
            std::make_shared<small::execution::QueryServiceImpl>(),
            std::make_shared<small::gossip::GossipServiceImpl>(),
            std::make_shared<small::catalog::CatalogServiceImpl>(),
            std::make_shared<small::txn::TxnServiceImpl>(),
        });

    struct sockaddr_in client_addr {};
    socklen_t client_len = sizeof(client_addr);

    char buffer[MAX_MESSAGE_LEN];
    memset(buffer, 0, sizeof(buffer));

    int sock_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_listen_fd < 0) {
        SPDLOG_ERROR("error creating socket: {}", strerror(errno));
        exit(EXIT_FAILURE);
    }
    int opt = 1;
    setsockopt(sock_listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    auto server_addr = small::util::ip::str_to_sockaddr(args.sql_addr);

    if (bind(sock_listen_fd, (struct sockaddr*)&server_addr,
             sizeof(server_addr)) < 0) {
        SPDLOG_ERROR("error binding socket {}, error: {}", args.sql_addr,
                     errno);
        return EXIT_FAILURE;
    }

    if (listen(sock_listen_fd, BACKLOG) < 0) {
        SPDLOG_ERROR("error listening: {}", strerror(errno));
    }
    SPDLOG_INFO("sql server listening on addr: {}", args.sql_addr);

    struct epoll_event ev {
    }, events[MAX_EVENTS];
    int new_events, sock_conn_fd, epollfd;

    epollfd = epoll_create(MAX_EVENTS);
    if (epollfd < 0) {
        SPDLOG_ERROR("SPDLOG_ERROR creating epoll..\n");
    }
    ev.events = EPOLLIN;
    ev.data.fd = sock_listen_fd;

    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, sock_listen_fd, &ev) == -1) {
        SPDLOG_ERROR("SPDLOG_ERROR adding new listeding socket to epoll..\n");
    }

    auto close_connection = [&epollfd](int fd, const std::string& reason) {
        SPDLOG_INFO("{} (fd={})", reason, fd);
        epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, nullptr);
        SocketsManager::remove_socket_state(fd);
        close(fd);
    };

    while (true) {
        if (stopSignal.load()) {
            SPDLOG_INFO("stop signal received, stopping the server");
            break;
        }

        new_events = epoll_wait(epollfd, events, MAX_EVENTS, 1000);

        if (new_events == -1) {
            SPDLOG_ERROR("SPDLOG_ERROR in epoll_wait..\n");
        }

        for (int i = 0; i < new_events; ++i) {
            if (events[i].data.fd == sock_listen_fd) {
                sock_conn_fd =
                    accept4(sock_listen_fd, (struct sockaddr*)&client_addr,
                            &client_len, SOCK_NONBLOCK);
                if (sock_conn_fd == -1) {
                    SPDLOG_ERROR("SPDLOG_ERROR accepting new connection..\n");
                }

                ev.events = EPOLLIN | EPOLLET;
                ev.data.fd = sock_conn_fd;
                if (epoll_ctl(epollfd, EPOLL_CTL_ADD, sock_conn_fd, &ev) ==
                    -1) {
                    SPDLOG_ERROR("SPDLOG_ERROR adding new event to epoll..\n");
                }
            } else {
                int fd = events[i].data.fd;
                try {
                    if (!handle_client_event(fd)) {
                        close_connection(fd, "client disconnected");
                    }
                } catch (const std::exception& e) {
                    close_connection(fd, e.what());
                }
            }
        }
    }

    close(sock_listen_fd);
    return 0;
}

void StopServer() {
    SPDLOG_INFO("stopping the server");
    stopSignal = true;
}

}  // namespace small::server
