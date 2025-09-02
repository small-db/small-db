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

#include <string>
#include <vector>
#include <filesystem>

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

// Function to check if a directory is a valid RocksDB directory
bool IsRocksDBDirectory(const std::string& dir_path) {
    try {
        // Try to open the directory as a RocksDB database
        auto db = small::rocks::RocksDBWrapper::GetInstance(dir_path, {});
        return true;
    } catch (const std::exception& e) {
        // If opening fails, it's not a valid RocksDB directory
        return false;
    }
}

// Function to find all RocksDB directories under a given path with depth limit of 1
std::vector<std::string> FindRocksDBDirectories(const std::string& base_path) {
    std::vector<std::string> rocksdb_dirs;
    
    // Check if base directory exists
    if (!std::filesystem::exists(base_path)) {
        return rocksdb_dirs;
    }
    
    // Check the base directory itself
    if (IsRocksDBDirectory(base_path)) {
        rocksdb_dirs.push_back(base_path);
    }
    
    // Check immediate subdirectories (depth 1)
    try {
        for (const auto& entry : std::filesystem::directory_iterator(base_path)) {
            if (entry.is_directory()) {
                std::string subdir_path = entry.path().string();
                if (IsRocksDBDirectory(subdir_path)) {
                    rocksdb_dirs.push_back(subdir_path);
                }
            }
        }
    } catch (const std::filesystem::filesystem_error& e) {
        SPDLOG_WARN("Error scanning directory {}: {}", base_path, e.what());
    }
    
    return rocksdb_dirs;
}

// take args:
// - prefix
int main(int argc, char** argv) {
    CLI::App app{"RocksDB scan"};

    // scan prefix, optional, default is empty
    std::string prefix;
    app.add_option("--prefix", prefix, "Scan prefix")->default_str("");
    
    // data directory path, optional, default is ./data
    std::string data_path = "./data";
    app.add_option("--data-path", data_path, "Data directory path")->default_str("./data");

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& e) {
        return app.exit(e);
    }

    // Find all RocksDB directories dynamically
    std::vector<std::string> data_dir_list = FindRocksDBDirectories(data_path);
    
    if (data_dir_list.empty()) {
        SPDLOG_INFO("No RocksDB directories found under {}", data_path);
        return 0;
    }

    for (const auto& data_dir : data_dir_list) {
        SPDLOG_INFO("scan data dir: {}", data_dir);
        auto db = small::rocks::RocksDBWrapper::GetInstance(data_dir, {});
        db->PrintAllKV();
    }

    return 0;
}
