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
#include <sys/timex.h>
#include <unistd.h>

// =====================================================================
// c++ std
// =====================================================================

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>

// =====================================================================
// third-party libraries
// =====================================================================

// CLI11
#include "CLI/CLI.hpp"

// spdlog
#include "spdlog/spdlog.h"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/clock_skew/wire.h"

namespace {

std::string read_file_trim(const std::string& path) {
    std::ifstream in(path);
    if (!in) return {};
    std::stringstream ss;
    ss << in.rdbuf();
    std::string s = ss.str();
    while (!s.empty() && (s.back() == '\n' || s.back() == ' ' ||
                          s.back() == '\t' || s.back() == '\r')) {
        s.pop_back();
    }
    return s;
}

std::string get_hostname() {
    std::array<char, 256> buf{};
    if (gethostname(buf.data(), buf.size()) != 0) return {};
    buf[buf.size() - 1] = '\0';
    return std::string(buf.data());
}

uint64_t now_realtime_ns() {
    struct timespec ts {};
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL +
           static_cast<uint64_t>(ts.tv_nsec);
}

// Snapshot kernel NTP state via adjtimex(2). offset_ns is the kernel's
// current estimate of how far the local clock is from the NTP source;
// status is the adjtimex return code (TIME_OK, TIME_ERROR, ...). If
// adjtimex fails we report status = -1 and offset_ns = 0.
void snapshot_ntp(int64_t* offset_ns, int32_t* status) {
    struct timex tx {};
    tx.modes = 0;
    int rc = adjtimex(&tx);
    if (rc < 0) {
        *offset_ns = 0;
        *status = -1;
        return;
    }
    int64_t offset = tx.offset;
    if (!(tx.status & STA_NANO)) offset *= 1000;
    *offset_ns = offset;
    *status = rc;
}

void append(std::string* buf, const void* data, size_t n) {
    buf->append(static_cast<const char*>(data), n);
}

template <typename T>
void append_pod(std::string* buf, const T& v) {
    append(buf, &v, sizeof(T));
}

void append_lp_string(std::string* buf, const std::string& s) {
    auto len = static_cast<uint32_t>(s.size());
    append_pod(buf, len);
    append(buf, s.data(), s.size());
}

}  // namespace

int main(int argc, char* argv[]) {
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");

    CLI::App app{
        "clock_skew_server: tiny UDP daemon that answers with current "
        "wall-clock time + clocksource info"};
    std::string bind_addr = "0.0.0.0";
    int bind_port = 12321;
    app.add_option("--bind", bind_addr, "Bind address (default 0.0.0.0)")
        ->capture_default_str();
    app.add_option("--port", bind_port, "UDP port (default 12321)")
        ->capture_default_str();
    CLI11_PARSE(app, argc, argv);

    const std::string hostname = get_hostname();
    const std::string current_cs = read_file_trim(
        "/sys/devices/system/clocksource/clocksource0/current_clocksource");
    const std::string available_cs = read_file_trim(
        "/sys/devices/system/clocksource/clocksource0/available_clocksource");

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        spdlog::error("socket: {}", std::strerror(errno));
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(bind_port));
    if (inet_pton(AF_INET, bind_addr.c_str(), &addr.sin_addr) != 1) {
        spdlog::error("invalid bind address: {}", bind_addr);
        return 1;
    }
    if (bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        spdlog::error("bind {}:{}: {}", bind_addr, bind_port,
                      std::strerror(errno));
        return 1;
    }

    spdlog::info(
        "clock_skew_server listening on {}:{}  hostname={}  clocksource={}",
        bind_addr, bind_port, hostname, current_cs);

    std::array<char, 1024> req_buf{};
    while (true) {
        sockaddr_in peer{};
        socklen_t peer_len = sizeof(peer);
        ssize_t n = recvfrom(sock, req_buf.data(), req_buf.size(), 0,
                             reinterpret_cast<sockaddr*>(&peer), &peer_len);
        uint64_t t_recv = now_realtime_ns();
        if (n < static_cast<ssize_t>(sizeof(small::clock_skew::Request))) {
            continue;
        }
        small::clock_skew::Request req{};
        std::memcpy(&req, req_buf.data(), sizeof(req));
        if (req.magic != small::clock_skew::kWireMagic) continue;

        int64_t ntp_offset_ns = 0;
        int32_t ntp_status = -1;
        snapshot_ntp(&ntp_offset_ns, &ntp_status);

        std::string out;
        out.reserve(256 + hostname.size() + current_cs.size() +
                    available_cs.size());
        small::clock_skew::ResponseHeader hdr{};
        hdr.magic = small::clock_skew::kWireMagic;
        hdr.client_send_ns_echo = req.client_send_ns;
        hdr.server_recv_ns = t_recv;
        hdr.ntp_offset_ns = ntp_offset_ns;
        hdr.ntp_status = ntp_status;

        // Stamp t_send as late as possible: build the variable tail
        // first, then patch t_send into the header just before sendto.
        append_pod(&out, hdr);
        append_lp_string(&out, hostname);
        append_lp_string(&out, current_cs);
        append_lp_string(&out, available_cs);

        uint64_t t_send = now_realtime_ns();
        std::memcpy(out.data() + offsetof(small::clock_skew::ResponseHeader,
                                          server_send_ns),
                    &t_send, sizeof(t_send));

        ssize_t s = sendto(sock, out.data(), out.size(), 0,
                           reinterpret_cast<sockaddr*>(&peer), peer_len);
        if (s < 0) {
            spdlog::warn("sendto: {}", std::strerror(errno));
        }
    }

    return 0;
}
