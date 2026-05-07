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

#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "CLI/CLI.hpp"
#include "nlohmann/json.hpp"
#include "spdlog/fmt/fmt.h"

#include "src/clock_skew/wire.h"

namespace {

uint64_t now_realtime_ns() {
    struct timespec ts {};
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL +
           static_cast<uint64_t>(ts.tv_nsec);
}

struct Target {
    std::string name;
    std::string host;
    uint16_t port{};
};

// Parse `name=host:port`. Anything else is rejected with a brief error.
std::optional<Target> parse_target(const std::string& spec, std::string* err) {
    auto eq = spec.find('=');
    if (eq == std::string::npos) {
        *err = "expected name=host:port, got: " + spec;
        return std::nullopt;
    }
    std::string name = spec.substr(0, eq);
    std::string rest = spec.substr(eq + 1);
    auto colon = rest.rfind(':');
    if (colon == std::string::npos) {
        *err = "expected host:port after =, got: " + rest;
        return std::nullopt;
    }
    Target t;
    t.name = name;
    t.host = rest.substr(0, colon);
    try {
        t.port = static_cast<uint16_t>(std::stoi(rest.substr(colon + 1)));
    } catch (...) {
        *err = "bad port in: " + rest;
        return std::nullopt;
    }
    return t;
}

struct ProbeResult {
    bool ok = false;
    std::string error;
    std::string hostname;
    std::string clocksource;
    std::string available_clocksources;
    int64_t ntp_offset_ns = 0;
    int32_t ntp_status = -1;
    int samples_attempted = 0;
    int samples_received = 0;
    int64_t rtt_ns = 0;     // min RTT across received samples
    int64_t offset_ns = 0;  // offset paired with the min-RTT sample
};

// Read a length-prefixed string (u32 length, then bytes) starting at *cursor;
// advance *cursor on success. Returns false if the buffer is too short.
bool read_lp_string(const char* buf, size_t buf_len, size_t* cursor,
                    std::string* out) {
    if (*cursor + sizeof(uint32_t) > buf_len) return false;
    uint32_t len = 0;
    std::memcpy(&len, buf + *cursor, sizeof(len));
    *cursor += sizeof(len);
    if (*cursor + len > buf_len) return false;
    out->assign(buf + *cursor, len);
    *cursor += len;
    return true;
}

ProbeResult probe(const Target& target, int samples,
                  std::chrono::milliseconds recv_timeout) {
    ProbeResult r;
    r.samples_attempted = samples;

    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    addrinfo* res = nullptr;
    int gai = getaddrinfo(target.host.c_str(),
                          std::to_string(target.port).c_str(), &hints, &res);
    if (gai != 0) {
        r.error = fmt::format("getaddrinfo: {}", gai_strerror(gai));
        return r;
    }

    int sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sock < 0) {
        r.error = fmt::format("socket: {}", std::strerror(errno));
        freeaddrinfo(res);
        return r;
    }

    timeval tv{};
    tv.tv_sec = recv_timeout.count() / 1000;
    tv.tv_usec = (recv_timeout.count() % 1000) * 1000;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    int64_t best_rtt = std::numeric_limits<int64_t>::max();
    int64_t best_offset = 0;

    for (int i = 0; i < samples; ++i) {
        small::clock_skew::Request req{};
        req.magic = small::clock_skew::kWireMagic;
        req._pad = 0;

        uint64_t t1 = now_realtime_ns();
        req.client_send_ns = t1;
        ssize_t s = sendto(sock, &req, sizeof(req), 0, res->ai_addr,
                           res->ai_addrlen);
        if (s != static_cast<ssize_t>(sizeof(req))) {
            continue;
        }

        std::array<char, 2048> buf{};
        ssize_t n = recvfrom(sock, buf.data(), buf.size(), 0, nullptr, nullptr);
        uint64_t t4 = now_realtime_ns();
        if (n < static_cast<ssize_t>(
                    sizeof(small::clock_skew::ResponseHeader))) {
            continue;
        }

        small::clock_skew::ResponseHeader hdr{};
        std::memcpy(&hdr, buf.data(), sizeof(hdr));
        if (hdr.magic != small::clock_skew::kWireMagic) continue;
        if (hdr.client_send_ns_echo != t1) continue;  // stale reply

        size_t cursor = sizeof(hdr);
        std::string hostname, clocksource, available;
        if (!read_lp_string(buf.data(), n, &cursor, &hostname)) continue;
        if (!read_lp_string(buf.data(), n, &cursor, &clocksource)) continue;
        if (!read_lp_string(buf.data(), n, &cursor, &available)) continue;

        auto t2 = static_cast<int64_t>(hdr.server_recv_ns);
        auto t3 = static_cast<int64_t>(hdr.server_send_ns);
        int64_t rtt = (static_cast<int64_t>(t4) - static_cast<int64_t>(t1)) -
                      (t3 - t2);
        int64_t offset = ((t2 - static_cast<int64_t>(t1)) +
                          (t3 - static_cast<int64_t>(t4))) /
                         2;

        r.samples_received++;
        if (rtt < best_rtt) {
            best_rtt = rtt;
            best_offset = offset;
            r.hostname = hostname;
            r.clocksource = clocksource;
            r.available_clocksources = available;
            r.ntp_offset_ns = hdr.ntp_offset_ns;
            r.ntp_status = hdr.ntp_status;
        }
    }

    close(sock);
    freeaddrinfo(res);

    if (r.samples_received == 0) {
        if (r.error.empty()) r.error = "no replies received";
        return r;
    }
    r.ok = true;
    r.rtt_ns = best_rtt;
    r.offset_ns = best_offset;
    return r;
}

const char* ntp_status_str(int32_t s) {
    switch (s) {
        case 0:
            return "TIME_OK";
        case 1:
            return "TIME_INS";
        case 2:
            return "TIME_DEL";
        case 3:
            return "TIME_OOP";
        case 4:
            return "TIME_WAIT";
        case 5:
            return "TIME_ERROR";
        default:
            return "UNKNOWN";
    }
}

void emit_human(const std::vector<std::pair<Target, ProbeResult>>& results) {
    fmt::print("{:<10} {:<22} {:>14} {:>12} {:<14} {:<10} {}\n", "name",
               "host:port", "offset_ms", "rtt_us", "clocksource", "ntp",
               "samples");
    for (const auto& [t, r] : results) {
        std::string addr = fmt::format("{}:{}", t.host, t.port);
        if (!r.ok) {
            fmt::print("{:<10} {:<22} {}\n", t.name, addr,
                       fmt::format("ERROR: {}", r.error));
            continue;
        }
        fmt::print("{:<10} {:<22} {:>14.3f} {:>12.1f} {:<14} {:<10} {}/{}\n",
                   t.name, addr, r.offset_ns / 1e6, r.rtt_ns / 1e3,
                   r.clocksource, ntp_status_str(r.ntp_status),
                   r.samples_received, r.samples_attempted);
    }
}

void emit_json(const std::vector<std::pair<Target, ProbeResult>>& results) {
    nlohmann::json out;
    out["ts_ns"] = now_realtime_ns();
    out["results"] = nlohmann::json::array();
    for (const auto& [t, r] : results) {
        nlohmann::json j;
        j["name"] = t.name;
        j["host"] = t.host;
        j["port"] = t.port;
        j["ok"] = r.ok;
        j["samples_attempted"] = r.samples_attempted;
        j["samples_received"] = r.samples_received;
        if (r.ok) {
            j["hostname"] = r.hostname;
            j["clocksource"] = r.clocksource;
            j["available_clocksources"] = r.available_clocksources;
            j["ntp_offset_ns"] = r.ntp_offset_ns;
            j["ntp_status"] = r.ntp_status;
            j["ntp_status_name"] = ntp_status_str(r.ntp_status);
            j["rtt_ns"] = r.rtt_ns;
            j["offset_ns"] = r.offset_ns;
        } else {
            j["error"] = r.error;
        }
        out["results"].push_back(j);
    }
    std::cout << out.dump() << '\n';
}

}  // namespace

int main(int argc, char* argv[]) {
    CLI::App app{
        "clock_skew_client: probe one or more clock_skew_servers and report "
        "offset (Cristian's algorithm, min-RTT filter)"};

    std::vector<std::string> raw_targets;
    int samples = 16;
    int recv_timeout_ms = 200;
    std::string format = "human";

    app.add_option("--target", raw_targets,
                   "Probe target as name=host:port. Repeat for each peer.")
        ->required();
    app.add_option("--samples", samples,
                   "Probes per target (min RTT wins)")
        ->capture_default_str();
    app.add_option("--recv-timeout-ms", recv_timeout_ms,
                   "Per-probe receive timeout")
        ->capture_default_str();
    app.add_option("--format", format, "human | json")->capture_default_str();
    CLI11_PARSE(app, argc, argv);

    std::vector<Target> targets;
    for (const auto& raw : raw_targets) {
        std::string err;
        auto t = parse_target(raw, &err);
        if (!t) {
            fmt::print(stderr, "bad --target: {}\n", err);
            return 2;
        }
        targets.push_back(*t);
    }

    std::vector<std::pair<Target, ProbeResult>> results;
    results.reserve(targets.size());
    for (const auto& t : targets) {
        auto r = probe(t, samples, std::chrono::milliseconds(recv_timeout_ms));
        results.emplace_back(t, std::move(r));
    }

    if (format == "json") {
        emit_json(results);
    } else if (format == "human") {
        emit_human(results);
    } else {
        fmt::print(stderr, "unknown --format: {} (want human|json)\n", format);
        return 2;
    }
    return 0;
}
