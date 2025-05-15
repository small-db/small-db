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
// local libraries
// =====================================================================

#include "src/catalog/catalog.h"
#include "src/gossip/gossip.h"
#include "src/insert/insert.h"
#include "src/peers/server_registry.h"
#include "src/pg_wire/pg_wire.h"
#include "src/server/stmt_handler.h"
#include "src/server_info/info.h"
#include "src/util/ip/ip.h"

// =====================================================================
// self header
// =====================================================================

#include "src/server/server.h"

#define BACKLOG 512
#define MAX_EVENTS 128
#define MAX_MESSAGE_LEN 2048

namespace small::server {

class Server {
   private:
    // singleton instance - the only instance
    static Server* instance_ptr;

    // singleton instance - protected destructor
    ~Server() = default;

    explicit Server(const small::server_info::ImmutableInfo& info)
        : info(info) {
        this->gossip_server = new small::gossip::GossipServer(info);
    }

    small::server_info::ImmutableInfo info;

    small::gossip::GossipServer* gossip_server;

   public:
    // singleton instance - assignment-blocker
    void operator=(const Server&) = delete;

    // singleton instance - copy-blocker
    Server(const Server&) = delete;

    // singleton instance - init api
    static void init_instance(const small::server_info::ImmutableInfo& info);

    // singleton instance - get api
    static Server* get_instance();
};

std::atomic<bool> stopSignal = false;

class ReaderWriter {
   protected:
    static int32_t read_int32(int sockfd) {
        int32_t network_value;
        ssize_t bytes_received =
            recv(sockfd, &network_value, sizeof(network_value), 0);
        if (bytes_received != sizeof(network_value)) {
            throw std::runtime_error("error reading int32_t from socket..");
        }
        int32_t value = ntohl(network_value);
        return value;
    }
};

int32_t read_int32_chars(char* buffer) {
    int32_t network_value;
    memcpy(&network_value, buffer, sizeof(network_value));
    int32_t value = ntohl(network_value);
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

    // Static pointer to the Singleton instance.
    static SocketsManager* instancePtr;

    // Mutex to ensure thread safety.
    static std::mutex mtx;

    // Private Constructor
    SocketsManager() {}

   public:
    /**
     * Delete the assignment operator.
     */
    void operator=(const SocketsManager&) = delete;

    /**
     * Delete the copy constructor.
     */
    SocketsManager(const SocketsManager& obj) = delete;

    // Static method to get the Singleton instance
    static SocketsManager* getInstance() {
        if (instancePtr == nullptr) {
            std::lock_guard<std::mutex> lock(mtx);
            if (instancePtr == nullptr) {
                instancePtr = new SocketsManager();
            }
        }
        return instancePtr;
    }

    // TODO: protect the access to the socket_states map with a mutex
    static SocketState get_socket_state(int sockfd) {
        auto instance = getInstance();

        auto it = instance->socket_states.find(sockfd);
        if (it == instance->socket_states.end()) {
            // set the initial state
            instance->socket_states[sockfd] = SocketState::StartUp;
            return SocketState::StartUp;
        }

        return it->second;
    }

    // TODO: protect the access to the socket_states map with a mutex
    static void set_socket_state(int sockfd, SocketState state) {
        auto instance = getInstance();
        instance->socket_states[sockfd] = state;
    }

    static void remove_socket_state(int sockfd) {
        auto instance = getInstance();
        instance->socket_states.erase(sockfd);
    }
};

// define the static members
SocketsManager* SocketsManager::instancePtr = nullptr;
std::mutex SocketsManager::mtx;

// get a message with length word from connection
std::string pq_getmessage(char* buffer) {
    int len = read_int32_chars(buffer);
    std::string message(buffer + 4, len);
    return message;
}

class SSLRequest : ReaderWriter {
   public:
    static const int BODY_SIZE = 8;
    static const int SSL_MAGIC_CODE = 80877103;

    static void handle_ssl_request(int newsockfd) {
        SPDLOG_DEBUG("handling ssl request, newsockfd: {}", newsockfd);

        auto body_size = read_int32(newsockfd);
        if (body_size != BODY_SIZE) {
            auto error_msg =
                fmt::format("invalid length of startup packet: {}", body_size);
            throw std::runtime_error(error_msg);
        }

        auto ssl_code = read_int32(newsockfd);
        if (ssl_code != SSL_MAGIC_CODE) {
            auto error_msg = fmt::format("invalid ssl code: {}", ssl_code);
            throw std::runtime_error(error_msg);
        }

        // reply 'N' for no SSL support
        char SSLok = 'N';
        send(newsockfd, &SSLok, 1, 0);
    }
};

void handle_query(std::string& query, int sockfd) {
    SPDLOG_INFO("query: {}", query);

    PgQueryParseResult result;

    result = pg_query_parse(query.c_str());
    if (result.error != nullptr) {
        SPDLOG_ERROR("error parsing query: {}", result.error->message);
        small::pg_wire::send_error(sockfd, result.error->message);
        return;
    }
    SPDLOG_INFO("ast: {}", result.parse_tree);
    pg_query_free_parse_result(result);

    PgQueryProtobufParseResult pgquery_pbparse_result =
        pg_query_parse_protobuf_opts(query.c_str(), PG_QUERY_PARSE_DEFAULT);

    auto unpacked = pg_query__parse_result__unpack(
        NULL, pgquery_pbparse_result.parse_tree.len,
        (const uint8_t*)pgquery_pbparse_result.parse_tree.data);

    auto node_case = unpacked->stmts[0]->stmt->node_case;

    for (int i = 0; i < unpacked->n_stmts; i++) {
        auto result =
            small::stmt_handler::handle_stmt(unpacked->stmts[i]->stmt);
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
        SPDLOG_INFO("server started, address: {}", addr);
        server->Wait();
        SPDLOG_INFO("server stopped, address: {}", addr);
    }).detach();
}

int RunServer(const small::server_info::ImmutableInfo& args) {
    // === initialize singleton instances start ===
    auto status = small::server_info::init(args);
    if (!status.ok()) {
        SPDLOG_ERROR("failed to init server: {}", status.ToString());
        return EXIT_FAILURE;
    }

    small::catalog::Catalog::InitInstance();

    small::gossip::GossipServer::init_instance(args);
    // === initialize singleton instances end ===

    SPDLOG_INFO(
        "start server: sql_address: {}, grpc_address: {}, region: {}"
        " data_dir: {}",
        args.sql_addr, args.grpc_addr, args.region, args.data_dir);

    start_grpc_server(
        args.grpc_addr,
        {
            std::make_shared<small::server_registry::RegistryService>(),
            std::make_shared<insert::InsertService>(),
        });

    status = small::server_registry::join(args);
    if (!status.ok()) {
        SPDLOG_ERROR("failed to join peer: {}", status.ToString());
        return EXIT_FAILURE;
    }

    struct sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);

    char buffer[MAX_MESSAGE_LEN];
    memset(buffer, 0, sizeof(buffer));

    int sock_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_listen_fd < 0) {
        SPDLOG_ERROR("error creating socket: {}", strerror(errno));
        exit(EXIT_FAILURE);  // Exit the program if socket creation fails
    }
    int opt = 1;
    setsockopt(sock_listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    auto server_addr = small::util::ip::str_to_sockaddr(args.sql_addr);

    // bind socket and listen for connections
    if (bind(sock_listen_fd, (struct sockaddr*)&server_addr,
             sizeof(server_addr)) < 0) {
        std::string error_msg =
            fmt::format("error binding socket: {}", strerror(errno));
        SPDLOG_ERROR(error_msg);
        return EXIT_FAILURE;
    }

    if (listen(sock_listen_fd, BACKLOG) < 0) {
        SPDLOG_ERROR("error listening: {}", strerror(errno));
    }
    SPDLOG_INFO("server listening on addr: {}", args.sql_addr);

    struct epoll_event ev, events[MAX_EVENTS];
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

    while (1) {
        if (stopSignal.load()) {
            SPDLOG_INFO("stop signal received, stopping the server");
            break;
        }

        // timeout: 1000ms
        new_events = epoll_wait(epollfd, events, MAX_EVENTS, 1000);

        if (new_events == -1) {
            SPDLOG_ERROR("SPDLOG_ERROR in epoll_wait..\n");
        }

        for (int i = 0; i < new_events; ++i) {
            int event_fd = events[i].data.fd;

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
                int newsockfd = events[i].data.fd;

                auto state = SocketsManager::get_socket_state(newsockfd);
                switch (state) {
                    case SocketsManager::SocketState::StartUp: {
                        SSLRequest::handle_ssl_request(newsockfd);
                        SocketsManager::set_socket_state(
                            newsockfd,
                            SocketsManager::SocketState::NoSSLAcknowledged);
                        break;
                    }

                    case SocketsManager::SocketState::NoSSLAcknowledged: {
                        int bytes_received =
                            recv(newsockfd, buffer, MAX_MESSAGE_LEN, 0);

                        if (bytes_received < 0) {
                            // TODO(xiaochen):
                            // - add case "EAGAIN", which is the same as
                            // "EWOULDBLOCK"
                            switch (errno) {
                                case EWOULDBLOCK:
                                    // Non-blocking socket operation would block
                                    SPDLOG_DEBUG(
                                        "Would block, try again later");
                                    break;
                                case ECONNREFUSED:
                                    SPDLOG_DEBUG("Connection refused");
                                    // Handle reconnection logic here
                                    break;
                                case ETIMEDOUT:
                                    SPDLOG_DEBUG("Connection timed out");
                                    // Handle timeout logic here
                                    break;
                                case ENOTCONN:
                                    SPDLOG_DEBUG("Socket is not connected");
                                    // Handle disconnection logic here
                                    break;
                                default:
                                    SPDLOG_DEBUG("recv() failed: {}",
                                                 strerror(errno));
                                    // Handle other errors
                                    break;
                            }
                            continue;
                        }

                        std::string message = pq_getmessage(buffer);

                        // the first 4 bytes is version
                        std::string version(buffer + 4, 4);

                        std::unordered_map<std::string, std::string>
                            recv_params;
                        // key and value are separated by '\x00'
                        int pos = 8;  // start after the version
                        while (pos < bytes_received) {
                            std::string key;
                            std::string value;

                            // Read key
                            while (pos < bytes_received &&
                                   buffer[pos] != '\x00') {
                                key += buffer[pos];
                                pos++;
                            }
                            pos++;  // skip the null character

                            // Read value
                            while (pos < bytes_received &&
                                   buffer[pos] != '\x00') {
                                value += buffer[pos];
                                pos++;
                            }
                            pos++;  // skip the null character

                            if (!key.empty()) {
                                recv_params[key] = value;
                            }
                        }

                        small::pg_wire::send_ready(newsockfd);

                        SocketsManager::set_socket_state(
                            newsockfd,
                            SocketsManager::SocketState::ReadyForQuery);
                        break;
                    }
                    case SocketsManager::SocketState::ReadyForQuery: {
                        std::vector<char> buffer(MAX_MESSAGE_LEN);
                        int bytes_received =
                            recv(newsockfd, buffer.data(), buffer.size(), 0);
                        if (bytes_received < 0) {
                            spdlog::error("error receiving data: {}",
                                          strerror(errno));
                            close(newsockfd);
                            continue;
                        } else if (bytes_received == 0) {
                            spdlog::info("connection closed by peer");
                            close(newsockfd);
                            continue;
                        }

                        char message_type = buffer[0];
                        switch (message_type) {
                            case 'Q': {
                                // Query

                                // read length of the query
                                int32_t query_len =
                                    read_int32_chars(buffer.data() + 1);

                                // read the query
                                std::string query(buffer.data() + 5,
                                                  query_len - 4);
                                handle_query(query, newsockfd);
                                break;
                            }

                            case 'X': {
                                // Terminate
                                SPDLOG_INFO("terminate connection");
                                close(newsockfd);
                                SocketsManager::remove_socket_state(newsockfd);
                                break;
                            }

                            default:
                                SPDLOG_ERROR("unknown message type: {}",
                                             message_type);
                                exit(EXIT_FAILURE);
                                break;
                        }
                        break;
                    }

                    default:
                        SPDLOG_ERROR("unknown socket state: {}",
                                     SocketsManager::format(state));
                        exit(EXIT_FAILURE);
                        break;
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
