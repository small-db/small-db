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
// c system
// =====================================================================

#include <sys/wait.h>

// =====================================================================
// c++ std
// =====================================================================

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

// =====================================================================
// third-party libraries
// =====================================================================

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "spdlog/spdlog.h"
#include "src/util/narrow/narrow.h"

#include "CLI/CLI.hpp"
#include "nlohmann/json.hpp"

namespace fs = std::filesystem;

// =====================================================================
// hardcoded configuration
//
// All paths/keys/users are baked in so the tool can be invoked with zero
// arguments. Override only via the (intentionally tiny) CLI surface in main.
// =====================================================================

namespace config {

// Local data dirs to try. Both are checked; whatever exists is scanned.
//   - ./data             — three-region dev setup from scripts/test/test.sh
//   - /opt/small-db/data  — single-server data dir on a Jepsen VM
const std::vector<std::string> kLocalDataPathCandidates = {
    "./data",
    "/opt/small-db/data",
};

// Mirrors small-db-jepsen/src/small_db_jepsen/runner.clj. Hostnames assume
// /etc/hosts entries (america/europe/asia) created by `hostctl add` per
// CLAUDE.md.
constexpr const char* kSSHUser = "vagrant";
// Resolved as $HOME + this suffix.
constexpr const char* kSSHKeyRel = "/.vagrant.d/insecure_private_key";
constexpr const char* kRemoteBinary = "/opt/small-db/rocks_scan";
constexpr const char* kRemoteLibDir = "/opt/small-db/lib";
constexpr int kSSHConnectTimeoutSec = 5;

}  // namespace config

static const std::vector<std::string> kVagrantHosts = {
    "america",
    "europe",
    "asia",
};

// =====================================================================
// key parsing
//
// small-db keys have the form:
//   /<table_name>/<primary_key>/<20-digit-zero-padded-millis>
// e.g.
//   /default_schema.users/2/00000001777173141777
// =====================================================================

struct ParsedKey {
    std::string table;
    std::string pk;
    std::string ts_raw;
};

std::optional<ParsedKey> ParseKey(const std::string& key) {
    if (key.empty() || key[0] != '/') return std::nullopt;
    auto p1 = key.find('/', 1);
    if (p1 == std::string::npos) return std::nullopt;
    auto p2 = key.find('/', p1 + 1);
    if (p2 == std::string::npos) return std::nullopt;
    ParsedKey out;
    out.table = key.substr(1, p1 - 1);
    out.pk = key.substr(p1 + 1, p2 - p1 - 1);
    out.ts_raw = key.substr(p2 + 1);
    return out;
}

std::string FormatTimestamp(const std::string& ts_raw) {
    try {
        int64_t ms = std::stoll(ts_raw);
        auto s = static_cast<time_t>(ms / 1000);
        int ms_part = static_cast<int>(ms % 1000);
        struct tm tm {};
        localtime_r(&s, &tm);
        char buf[32];
        std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
        char out[48];
        std::snprintf(out, sizeof(out), "%s.%03d", buf, ms_part);
        return out;
    } catch (...) {
        return ts_raw;
    }
}

std::string FormatValue(const std::string& value, size_t max_len) {
    std::string rendered;
    try {
        auto j = nlohmann::json::parse(value);
        if (j.is_object()) {
            std::ostringstream out;
            bool first = true;
            for (auto it = j.begin(); it != j.end(); ++it) {
                if (!first) out << "  ";
                first = false;
                out << it.key() << "=";
                if (it.value().is_string()) {
                    out << it.value().get<std::string>();
                } else {
                    out << it.value().dump();
                }
            }
            rendered = out.str();
        } else {
            rendered = j.dump();
        }
    } catch (...) {
        rendered = value;
    }
    if (max_len > 0 && rendered.size() > max_len) {
        rendered.resize(max_len);
        rendered += "  ...";
    }
    return rendered;
}

// =====================================================================
// rocksdb open/read (read-only, so it doesn't fight a live server)
// =====================================================================

bool LooksLikeRocksDB(const std::string& dir) {
    if (!fs::is_directory(dir)) return false;
    return fs::exists(dir + "/CURRENT");
}

std::vector<std::string> FindRocksDBDirs(const std::string& base) {
    std::vector<std::string> out;
    if (!fs::exists(base)) return out;
    if (LooksLikeRocksDB(base)) out.push_back(base);
    try {
        for (const auto& e : fs::directory_iterator(base)) {
            if (e.is_directory() && LooksLikeRocksDB(e.path().string())) {
                out.push_back(e.path().string());
            }
        }
    } catch (const fs::filesystem_error& e) {
        SPDLOG_WARN("scan {}: {}", base, e.what());
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<std::pair<std::string, std::string>> ReadAll(
    const std::string& db_path) {
    rocksdb::Options opts;
    opts.create_if_missing = false;
    rocksdb::DB* db = nullptr;
    auto status = rocksdb::DB::OpenForReadOnly(opts, db_path, &db);
    if (!status.ok()) {
        SPDLOG_ERROR("open read-only {}: {}", db_path, status.ToString());
        return {};
    }
    std::vector<std::pair<std::string, std::string>> out;
    {
        // Iterator must be destroyed before the DB it was created from.
        rocksdb::ReadOptions ro;
        std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(ro));
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            out.emplace_back(it->key().ToString(), it->value().ToString());
        }
    }
    delete db;
    return out;
}

// =====================================================================
// pretty-print
// =====================================================================

void PrintHeader(const std::string& title) {
    constexpr int kWidth = 80;
    std::string prefix = "== " + title + " ";
    int pad = kWidth - static_cast<int>(prefix.size());
    if (pad < 3) pad = 3;
    std::cout << "\n" << prefix << std::string(pad, '=') << "\n";
}

struct ScanOptions {
    std::string prefix;
    bool latest_only = false;
    size_t max_value_len = 0;  // 0 = no truncation
};

void PrintScan(const std::vector<std::pair<std::string, std::string>>& kvs,
               const ScanOptions& opt) {
    // Group: table -> pk -> [(ts_raw, value)]
    std::map<
        std::string,
        std::map<std::string, std::vector<std::pair<std::string, std::string>>>>
        grouped;
    std::vector<std::pair<std::string, std::string>> unparseable;

    for (const auto& [k, v] : kvs) {
        if (!opt.prefix.empty() && k.rfind(opt.prefix, 0) != 0) continue;
        auto pk = ParseKey(k);
        if (!pk) {
            unparseable.emplace_back(k, v);
            continue;
        }
        grouped[pk->table][pk->pk].emplace_back(pk->ts_raw, v);
    }

    int row_count = 0;
    int version_count = 0;
    for (auto& [table, pks] : grouped) {
        std::cout << "\n  " << table << "\n";
        for (auto& [pk, versions] : pks) {
            std::sort(versions.begin(), versions.end());
            std::cout << "    pk=" << pk << "  (" << versions.size()
                      << " version" << (versions.size() == 1 ? "" : "s")
                      << ")\n";
            size_t start = (opt.latest_only && versions.size() > 1)
                               ? versions.size() - 1
                               : 0;
            for (size_t i = start; i < versions.size(); ++i) {
                const auto& [ts, val] = versions[i];
                bool is_latest = (i + 1 == versions.size());
                std::cout << "      [" << FormatTimestamp(ts) << "]  "
                          << FormatValue(val, opt.max_value_len);
                if (is_latest && versions.size() > 1) {
                    std::cout << "  <- latest";
                }
                std::cout << "\n";
            }
            ++row_count;
            version_count += small::util::narrow_cast<int>(versions.size());
        }
    }

    if (!unparseable.empty()) {
        std::cout << "\n  unparseable keys (" << unparseable.size() << "):\n";
        for (const auto& [k, v] : unparseable) {
            std::cout << "    " << k << "  =>  "
                      << FormatValue(v, opt.max_value_len) << "\n";
        }
    }

    std::cout << "\n  summary: " << row_count << " row(s), " << version_count
              << " version(s)";
    if (!unparseable.empty()) {
        std::cout << ", " << unparseable.size() << " unparseable";
    }
    std::cout << "\n";
}

// =====================================================================
// vagrant remote scan
// =====================================================================

// Shell-escape: wrap in single quotes and replace ' with '\''.
std::string ShellQuote(const std::string& s) {
    std::string out = "'";
    for (char c : s) {
        if (c == '\'') {
            out += "'\\''";
        } else {
            out += c;
        }
    }
    out += "'";
    return out;
}

int RunRemoteScan(const std::string& host, const std::string& ssh_key,
                  const ScanOptions& opt) {
    // The remote rocks_scan finds its own data dir via the same
    // kLocalDataPathCandidates list, so we don't need to pass --data-path.
    std::ostringstream remote;
    remote << "LD_LIBRARY_PATH=" << config::kRemoteLibDir << " "
           << config::kRemoteBinary;
    if (!opt.prefix.empty()) remote << " --prefix " << ShellQuote(opt.prefix);
    if (opt.latest_only) remote << " --latest";

    std::ostringstream cmd;
    cmd << "ssh -o StrictHostKeyChecking=no -o BatchMode=yes "
        << "-o ConnectTimeout=" << config::kSSHConnectTimeoutSec
        << " -o LogLevel=ERROR -i " << ShellQuote(ssh_key) << " "
        << config::kSSHUser << "@" << host << " " << ShellQuote(remote.str())
        << " 2>&1";

    PrintHeader("vm:" + host);
    FILE* p = popen(cmd.str().c_str(), "r");
    if (!p) {
        std::cerr << "  popen failed for " << host << "\n";
        return 1;
    }
    char buf[4096];
    while (std::fgets(buf, sizeof(buf), p)) {
        std::cout << buf;
    }
    int rc = pclose(p);
    if (rc != 0) {
        std::cerr << "  ssh to " << host
                  << " exited rc=" << (WIFEXITED(rc) ? WEXITSTATUS(rc) : -1)
                  << "\n";
    }
    return rc;
}

// =====================================================================
// main
// =====================================================================

int main(int argc, char** argv) {
    CLI::App app{
        "RocksDB scan: pretty-print small-db data (local + Jepsen VMs).\n"
        "Run with no arguments by default."};

    std::string prefix;
    bool latest_only = false;

    app.add_option("--prefix", prefix,
                   "Filter: only print keys with this prefix");
    app.add_flag("--latest", latest_only,
                 "Show only the most recent version of each row");

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& e) {
        return app.exit(e);
    }

    spdlog::set_pattern("[%^%l%$] %v");
    spdlog::set_level(spdlog::level::warn);

    ScanOptions scan_opt{prefix, latest_only, /*max_value_len=*/0};

    // ---- local: scan whichever candidate path exists ----
    bool any_local = false;
    for (const auto& path : config::kLocalDataPathCandidates) {
        for (const auto& d : FindRocksDBDirs(path)) {
            any_local = true;
            PrintHeader("local:" + d);
            auto kvs = ReadAll(d);
            if (kvs.empty()) {
                std::cout << "  (empty)\n";
            } else {
                PrintScan(kvs, scan_opt);
            }
        }
    }
    if (!any_local) {
        std::cout << "(no local RocksDB directories found in:";
        for (const auto& p : config::kLocalDataPathCandidates) {
            std::cout << " " << p;
        }
        std::cout << ")\n";
    }

    // ---- remote: skip silently if we have no Vagrant SSH key (e.g., we
    //      are running this binary inside one of the VMs themselves) ----
    const char* home = std::getenv("HOME");
    std::string ssh_key =
        (home ? std::string(home) : std::string()) + config::kSSHKeyRel;
    if (!fs::exists(ssh_key)) {
        return 0;
    }
    for (const auto& host : kVagrantHosts) {
        RunRemoteScan(host, ssh_key, scan_opt);
    }
    return 0;
}
