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

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

// arrow
#include "arrow/api.h"

// spdlog
#include "spdlog/fmt/bin_to_hex.h"
#include "spdlog/spdlog.h"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/type/type.h"

// =====================================================================
// self header
// =====================================================================

#include "src/pg_wire/pg_wire.h"

namespace small::pg_wire {
using std::string;

class ServerMessage {
   protected:
    static void append_char(std::vector<char>& buffer, char value) {
        buffer.push_back(value);
    }

    static void append_int16(std::vector<char>& buffer, int16_t value) {
        int16_t network_value = htons(value);
        const char* data = reinterpret_cast<const char*>(&network_value);
        buffer.insert(buffer.end(), data, data + sizeof(network_value));
    }

    static void append_int32(std::vector<char>& buffer, int32_t value) {
        int32_t network_value = htonl(value);
        const char* data = reinterpret_cast<const char*>(&network_value);
        buffer.insert(buffer.end(), data, data + sizeof(network_value));
    }

    static void write_int32(std::vector<char>& buffer, int32_t value,
                            int offset) {
        int32_t network_value = htonl(value);
        const char* data = reinterpret_cast<const char*>(&network_value);
        memcpy(buffer.data() + offset, data, sizeof(network_value));
    }

    static void append_cstring(std::vector<char>& buffer,
                               const std::string& value) {
        buffer.insert(buffer.end(), value.begin(), value.end());
        buffer.push_back('\x00');
    }

    static void append_vector(std::vector<char>& buffer,
                              const std::vector<char>& value) {
        buffer.insert(buffer.end(), value.begin(), value.end());
    }

   public:
    virtual void encode(std::vector<char>& buffer) = 0;
};

class NoSSLSupport : public ServerMessage {
   public:
    NoSSLSupport() = default;
    void encode(std::vector<char>& buffer) override {
        append_char(buffer, 'N');
    }
};

class AuthenticationOk : public ServerMessage {
   public:
    AuthenticationOk() = default;
    void encode(std::vector<char>& buffer) override {
        append_char(buffer, 'R');
        append_int32(buffer, 8);
        append_int32(buffer, 0);
    }
};

class EmptyQueryResponse : public ServerMessage {
   public:
    EmptyQueryResponse() = default;
    void encode(std::vector<char>& buffer) override {
        append_char(buffer, 'I');
        append_int32(buffer, 4);
    }
};

class RowDescriptionResponse : public ServerMessage {
   private:
    const std::shared_ptr<arrow::Schema>& schema;

   public:
    explicit RowDescriptionResponse(
        const std::shared_ptr<arrow::Schema>& schema)
        : schema(schema) {}

    void encode(std::vector<char>& buffer) override {
        append_char(buffer, 'T');

        // message length (placeholder)
        int pre_bytes = buffer.size();
        append_int32(buffer, 0);

        int16_t num_fields = schema->num_fields();
        append_int16(buffer, num_fields);

        for (int i = 0; i < num_fields; ++i) {
            const auto& field = schema->field(i);

            auto data_type =
                small::type::from_arrow_type(field->type()).value();

            // The field name.
            append_cstring(buffer, field->name());

            // The table OID.
            append_int32(buffer, 0);

            // The column attribute number.
            append_int16(buffer, 0);

            // The field's data type OID.
            append_int32(buffer, small::type::to_pgwire_oid(data_type));

            // The data type size.
            append_int16(buffer, small::type::get_pgwire_size(data_type));

            // The type modifier.
            append_int32(buffer, 0);

            // The format code. (0 for text, 1 for binary)
            append_int16(buffer, 0);
        }

        // update the message length
        int32_t message_length = buffer.size() - pre_bytes;
        write_int32(buffer, message_length, pre_bytes);
    }
};

// DataRow (B)
class DataRowResponse : public ServerMessage {
   private:
    const std::shared_ptr<arrow::RecordBatch>& batch;

   public:
    explicit DataRowResponse(const std::shared_ptr<arrow::RecordBatch>& batch)
        : batch(batch) {}

    void encode(std::vector<char>& buffer) {
        int num_rows = batch->num_rows();

        for (int i = 0; i < num_rows; ++i) {
            append_char(buffer, 'D');

            // message length (placeholder)
            int pre_bytes = buffer.size();
            append_int32(buffer, 0);

            // number of columns
            append_int16(buffer, batch->num_columns());

            for (int j = 0; j < batch->num_columns(); ++j) {
                std::string cell;
                auto col = batch->column(j);
                if (col->type_id() == arrow::Type::STRING) {
                    auto string_column =
                        std::static_pointer_cast<arrow::StringArray>(col);
                    cell = string_column->GetString(i);
                } else if (col->type_id() == arrow::Type::INT64) {
                    auto int_column =
                        std::static_pointer_cast<arrow::Int64Array>(col);
                    cell = std::to_string(int_column->Value(i));
                }
                append_int32(buffer, cell.size());
                buffer.insert(buffer.end(), cell.data(),
                              cell.data() + cell.size());
            }

            // update the message length
            int32_t message_length = buffer.size() - pre_bytes;
            write_int32(buffer, message_length, pre_bytes);
        }
    }
};

class CommandComplete : public ServerMessage {
   public:
    CommandComplete() = default;

    void encode(std::vector<char>& buffer) override {
        // DataRow (B)

        // identifier
        append_char(buffer, 'C');

        // message length (placeholder)
        int pre_bytes = buffer.size();
        append_int32(buffer, 0);

        // command tag
        append_cstring(buffer, "SELECT 0");

        // update the message length
        int32_t message_length = buffer.size() - pre_bytes;
        write_int32(buffer, message_length, pre_bytes);
    }
};

class ErrorResponse : public ServerMessage {
    enum class Severity {
        ERROR,
        FATAL,
        PANIC,
        WARNING,
        NOTICE,
        DEBUG,
        INFO,
        LOG,
    };

   private:
    const Severity severity;
    const std::string error_message;

   public:
    explicit ErrorResponse(const std::string& error_message = "error message")
        : severity(Severity::ERROR), error_message(error_message) {}

    explicit ErrorResponse(Severity severity = Severity::ERROR,
                           const std::string& error_message = "error message")
        : severity(severity), error_message(error_message) {}

    void encode(std::vector<char>& buffer) {
        append_char(buffer, 'E');

        std::vector<char> field_severity = encode_severity();
        std::vector<char> field_message = encode_message();

        int32_t message_length =
            4 + field_severity.size() + field_message.size() + 1;
        SPDLOG_DEBUG("message_length: {}", message_length);
        append_int32(buffer, message_length);
        append_vector(buffer, field_severity);
        append_vector(buffer, field_message);
        append_char(buffer, '\x00');

        SPDLOG_DEBUG(
            "error response: {}",
            spdlog::to_hex(buffer.data(), buffer.data() + buffer.size()));
        SPDLOG_DEBUG("error response: {}",
                     std::string(buffer.begin(), buffer.end()));
    }

    std::vector<char> encode_severity() {
        std::vector<char> buffer;

        append_char(buffer, 'S');
        switch (severity) {
            case Severity::DEBUG:
                append_cstring(buffer, "DEBUG");
                break;
            case Severity::INFO:
                append_cstring(buffer, "INFO");
                break;
            case Severity::ERROR:
                append_cstring(buffer, "ERROR");
                break;
            default:
                throw std::runtime_error("unsupported severity");
        }
        return buffer;
    }

    std::vector<char> encode_message() {
        std::vector<char> buffer;

        append_char(buffer, 'M');
        append_cstring(buffer, error_message);

        return buffer;
    }
};

class ParameterStatus : public ServerMessage {
    const std::string key;
    const std::string value;

   public:
    ParameterStatus(std::string key, std::string value)
        : key(std::move(key)), value(std::move(value)) {}

    // ParameterStatus (B)
    // Byte1('S')
    // Identifies the message as a run-time parameter status report.
    // Int32
    // Length of message contents in bytes, including self.
    // String
    // The name of the run-time parameter being reported.
    // String
    // The current value of the parameter.
    void encode(std::vector<char>& buffer) override {
        append_char(buffer, 'S');
        append_int32(buffer, 4 + key.size() + 1 + value.size() + 1);
        append_cstring(buffer, key);
        append_cstring(buffer, value);
    }
};

class BackendKeyData : public ServerMessage {
   public:
    BackendKeyData() = default;

    // BackendKeyData (B)
    // Byte1('K')
    // Identifies the message as cancellation key data. The frontend must save
    // these values if it wishes to be able to issue CancelRequest messages
    // later. Int32(12) Length of message contents in bytes, including self.
    // Int32
    // The process ID of this backend.
    // Int32
    // The secret key of this backend.
    void encode(std::vector<char>& buffer) override {
        append_char(buffer, 'K');
        append_int32(buffer, 12);

        int32_t process_id = getpid();
        append_int32(buffer, process_id);

        srand(time(nullptr));

        // TODO: use a thread-local seed
        unsigned int seed = 42;
        int32_t secret_key = rand_r(&seed);
        append_int32(buffer, secret_key);
    }
};

class ReadyForQuery : public ServerMessage {
   public:
    ReadyForQuery() = default;

    // ReadyForQuery (B)
    // Byte1('Z')
    // Identifies the message as a ready-for-query indicator.
    // Int32(5)
    // Length of message contents in bytes, including self.
    // Byte1
    // Current backend transaction status indicator. Possible values are 'I' if
    // idle (not in a transaction block); 'T' if in a transaction block; or 'E'
    // if in a failed transaction block (queries will be rejected until block is
    // ended).
    void encode(std::vector<char>& buffer) {
        append_char(buffer, 'Z');
        append_int32(buffer, 5);
        append_char(buffer, 'I');
    }
};

class NetworkPackage {
   private:
    std::vector<ServerMessage*> messages;

   public:
    NetworkPackage() = default;

    void add_message(ServerMessage* message) { messages.push_back(message); }

    void send_all(int sockfd) {
        std::vector<char> buffer;
        for (auto message : messages) {
            message->encode(buffer);
        }

        send(sockfd, buffer.data(), buffer.size(), 0);
    }
};

void send_no_ssl_support(int sockfd) {
    auto* network_package = new NetworkPackage();
    network_package->add_message(new NoSSLSupport());
    network_package->send_all(sockfd);
}

void send_ready(int sockfd) {
    auto* network_package = new NetworkPackage();
    network_package->add_message(new AuthenticationOk());

    std::unordered_map<std::string, std::string> params{
        {"server_encoding", "UTF8"}, {"client_encoding", "UTF8"},
        {"DateStyle", "ISO YMD"},    {"integer_datetimes", "on"},
        {"server_version", "17.0"},
    };
    for (const auto& kv : params) {
        network_package->add_message(new ParameterStatus(kv.first, kv.second));
    }
    network_package->add_message(new BackendKeyData());
    network_package->add_message(new ReadyForQuery());

    network_package->send_all(sockfd);
}

void send_batch(int sockfd, const std::shared_ptr<arrow::RecordBatch>& batch) {
    auto* network_package = new NetworkPackage();
    network_package->add_message(new RowDescriptionResponse(batch->schema()));
    network_package->add_message(new DataRowResponse(batch));
    network_package->add_message(new CommandComplete());
    network_package->add_message(new ReadyForQuery());
    network_package->send_all(sockfd);
}

void send_empty_result(int sockfd) {
    auto* network_package = new NetworkPackage();
    network_package->add_message(new EmptyQueryResponse());
    network_package->add_message(new ReadyForQuery());
    network_package->send_all(sockfd);
}

void send_error(int sockfd, const std::string& error_message) {
    auto* network_package = new NetworkPackage();
    network_package->add_message(new ErrorResponse(error_message));
    network_package->add_message(new ReadyForQuery());
    network_package->send_all(sockfd);
}

constexpr int MAX_MESSAGE_LEN = 2048;

uint32_t read_int32_chars(char* buffer) {
    uint32_t network_value;
    memcpy(&network_value, buffer, sizeof(network_value));
    uint32_t value = ntohl(network_value);
    return value;
}

// Returns the raw message bytes. Return empty string on client disconnect.
std::string read_bytes(int sockfd) {
    char buffer[MAX_MESSAGE_LEN];
    ssize_t bytes_received = recv(sockfd, buffer, MAX_MESSAGE_LEN, 0);

    if (bytes_received < 0) {
        std::string error_message =
            fmt::format("error receiving data: {}", strerror(errno));
        switch (errno) {
            case EWOULDBLOCK:
                // Non-blocking socket operation would block
                error_message += " (would block, try again later)";
                break;
            case ECONNREFUSED:
                error_message += " (connection refused)";
                // Handle reconnection logic here
                break;
            case ETIMEDOUT:
                error_message += " (connection timed out)";
                // Handle timeout logic here
                break;
            case ENOTCONN:
                error_message += " (socket is not connected)";
                // Handle disconnection logic here
                break;
            default:
                error_message +=
                    " (recv() failed: " + std::string(strerror(errno)) + ")";
                // Handle other errors
                break;
        }
        throw std::runtime_error(error_message);
    }

    // log in hex
    SPDLOG_INFO("received data in hex: {}",
                spdlog::to_hex(buffer, buffer + bytes_received));

    return std::string(buffer, bytes_received);
}

constexpr int SSL_MAGIC_CODE = 80877103;

int32_t read_int32(std::string& message, int offset) {
    int32_t network_value;
    memcpy(&network_value, message.data() + offset, sizeof(network_value));
    int32_t value = ntohl(network_value);
    return value;
}

std::optional<StartupPacketType> read_startup_packet(int sockfd) {
    std::string message = read_bytes(sockfd);
    if (message.size() < 8) {
        SPDLOG_INFO("startup packet too short: {} bytes (fd={})",
                     message.size(), sockfd);
        return std::nullopt;
    }

    if (message.size() == 8 && read_int32(message, 4) == SSL_MAGIC_CODE) {
        return StartupPacketType::SSLRequest;
    } else {
        // the first 4 bytes is length

        // the next 4 bytes is version
        std::string version(message.begin() + 4, message.begin() + 8);

        std::unordered_map<std::string, std::string> recv_params;
        // key and value are separated by '\x00'
        int pos = 8;  // start after the version
        while (pos < message.size()) {
            std::string key;
            std::string value;

            // Read key
            while (pos < message.size() && message[pos] != '\x00') {
                key += message[pos];
                pos++;
            }
            pos++;  // skip the null character

            // Read value
            while (pos < message.size() && message[pos] != '\x00') {
                value += message[pos];
                pos++;
            }
            pos++;  // skip the null character

            if (!key.empty()) {
                recv_params[key] = value;
            }
        }

        for (const auto& kv : recv_params) {
            SPDLOG_DEBUG("recv_param: {}={}", kv.first, kv.second);
        }

        return StartupPacketType::StartupMessage;
    }
}

}  // namespace small::pg_wire
