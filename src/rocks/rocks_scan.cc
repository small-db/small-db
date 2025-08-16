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
// third-party libraries
// =====================================================================

// spdlog
#include "spdlog/spdlog.h"

// CLI11
#include "CLI/CLI.hpp"

// =====================================================================
// local libraries
// =====================================================================

#include "src/rocks/rocks.h"

// take args:
// - prefix
int main(int argc, char** argv) {
    CLI::App app{"RocksDB scan"};

    // scan prefix, optional, default is empty
    std::string prefix;
    app.add_option("--prefix", prefix, "Scan prefix")->default_str("");

    std::vector<std::string> data_dir_list = {
        "./data/asia",
        "./data/eu",
        "./data/us",
    };

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& e) {
        return app.exit(e);
    }

    for (const auto& data_dir : data_dir_list) {
        auto db = small::rocks::RocksDBWrapper::GetInstance(data_dir, {});
        db->PrintAllKV();
    }

    return 0;
}
