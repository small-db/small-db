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
// c++ std
// =====================================================================

#include <csignal>
#include <cstdlib>
#include <string>

// =====================================================================
// third-party libraries
// =====================================================================

// spdlog
#include "spdlog/spdlog.h"

// CLI11
#include "CLI/CLI.hpp"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/server/server.h"

void shutdown_handler(int signum) {
    spdlog::info("server shutting down (signal: {})", signum);
    std::exit(signum);
}

int main(int argc, char *argv[]) {
    std::signal(SIGINT, shutdown_handler);
    std::signal(SIGTERM, shutdown_handler);
    std::atexit([] { spdlog::info("server exiting"); });

    spdlog::set_level(spdlog::level::debug);
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] [%@] %v");

    CLI::App app{"small-db"};

    std::string sql_addr;
    app.add_option("--sql-addr", sql_addr, "SQL address");

    std::string grpc_addr;
    app.add_option("--grpc-addr", grpc_addr, "gRPC address");

    std::string data_dir;
    app.add_option("--data-dir", data_dir, "Data directory")->required();

    std::string region;
    app.add_option("--region", region, "Region name");

    std::string join;
    app.add_option("--join", join, "Join server address");

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError &e) {
        return app.exit(e);
    }

    return small::server::RunServer(small::server_info::ImmutableInfo(
        sql_addr, grpc_addr, data_dir, region, join));
}
