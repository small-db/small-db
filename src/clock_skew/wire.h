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

#include <cstdint>

namespace small::clock_skew {

// Wire format for the clock-skew probe. Same-machine architecture on
// both ends (x86-64 little-endian Linux), so structs are sent raw with
// no byte-order conversion.
//
// Request (16 bytes):
//   u32 magic, u32 _pad, u64 client_send_ns
//
// Response = ResponseHeader followed by three length-prefixed strings:
//   ResponseHeader, u32 hostname_len, bytes hostname,
//   u32 current_clocksource_len, bytes current_clocksource,
//   u32 available_clocksources_len, bytes available_clocksources
constexpr uint32_t kWireMagic = 0x434B5357;  // "CKSW"

struct Request {
    uint32_t magic;
    uint32_t _pad;
    uint64_t client_send_ns;
};

struct ResponseHeader {
    uint32_t magic;
    uint32_t _pad;
    uint64_t client_send_ns_echo;
    uint64_t server_recv_ns;
    uint64_t server_send_ns;
    int64_t ntp_offset_ns;
    int32_t ntp_status;
    uint32_t _pad2;
};

}  // namespace small::clock_skew
