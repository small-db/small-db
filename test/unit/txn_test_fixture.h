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

// Shared fixture for unit tests that need a live small-db node.
//
// Why this much scaffolding for an "in-process" test:
// production write/read paths always go through gRPC. UPDATE fans out
// via gossip to every node (even when the only node is self), Query
// does the same, and intent resolution RPCs the coordinator address
// embedded in the intent. So a single-node test still needs every
// gRPC service registered on the configured port -- that is the
// minimum to make Txn::Execute work, not multi-node simulation.
//
// Tests should depend on TxnTestFixture and call Txn::* APIs; the
// gRPC details belong here, not in the test body.

#pragma once

// =====================================================================
// c++ std
// =====================================================================

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <thread>

// =====================================================================
// third-party libraries
// =====================================================================

#include "absl/status/status.h"
#include "grpcpp/server.h"
#include "grpcpp/server_builder.h"
#include "gtest/gtest.h"
#include "spdlog/spdlog.h"

// =====================================================================
// small-db libraries
// =====================================================================

#include "src/catalog/catalog.h"
#include "src/execution/insert.h"
#include "src/execution/query.h"
#include "src/execution/update.h"
#include "src/gossip/gossip.h"
#include "src/server_info/info.h"
#include "src/txn/handle.h"
#include "src/txn/txn.h"

namespace small::test {

// Stand up a single in-process small-db node: server_info, catalog,
// gossip, and every gRPC service the production code paths expect to
// be reachable. Lazily initialized once per process.
class InProcessNode {
 public:
    static InProcessNode& Instance() {
        static InProcessNode node;
        return node;
    }

    void EnsureStarted() {
        if (started_) return;
        started_ = true;

        spdlog::set_level(spdlog::level::info);
        spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] [%s:%#] %v");

        // Unique data dir + grpc port per process so concurrent test
        // binaries don't collide.
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> port_dist(40001, 49999);
        int grpc_port = port_dist(gen);

        std::string data_dir =
            "/tmp/small-db-unit-" + std::to_string(::getpid());
        std::filesystem::remove_all(data_dir);

        small::server_info::ImmutableInfo args(
            /*sql_addr=*/"127.0.0.1:0",
            /*grpc_addr=*/"127.0.0.1:" + std::to_string(grpc_port),
            /*data_dir=*/data_dir,
            /*region=*/"us",
            /*join=*/"");

        AssertOk(small::server_info::init(args));
        small::catalog::CatalogManager::InitInstance();
        small::gossip::GossipServer::init_instance(args, args.join);

        insert_svc_ = std::make_shared<small::execution::InsertServiceImpl>();
        update_svc_ = std::make_shared<small::execution::UpdateServiceImpl>();
        query_svc_ = std::make_shared<small::execution::QueryServiceImpl>();
        gossip_svc_ = std::make_shared<small::gossip::GossipServiceImpl>();
        catalog_svc_ = std::make_shared<small::catalog::CatalogServiceImpl>();
        txn_svc_ = std::make_shared<small::txn::TxnServiceImpl>();

        grpc::ServerBuilder builder;
        builder.AddListeningPort(args.grpc_addr,
                                 grpc::InsecureServerCredentials());
        builder.RegisterService(insert_svc_.get());
        builder.RegisterService(update_svc_.get());
        builder.RegisterService(query_svc_.get());
        builder.RegisterService(gossip_svc_.get());
        builder.RegisterService(catalog_svc_.get());
        builder.RegisterService(txn_svc_.get());

        grpc_server_ = builder.BuildAndStart();
        ASSERT_NE(grpc_server_, nullptr);

        // Give the gRPC accept loop a beat to be ready.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

 private:
    InProcessNode() = default;

    static void AssertOk(const absl::Status& s) {
        if (!s.ok()) {
            FAIL() << "init failed: " << s.ToString();
        }
    }

    bool started_ = false;
    std::shared_ptr<small::execution::InsertServiceImpl> insert_svc_;
    std::shared_ptr<small::execution::UpdateServiceImpl> update_svc_;
    std::shared_ptr<small::execution::QueryServiceImpl> query_svc_;
    std::shared_ptr<small::gossip::GossipServiceImpl> gossip_svc_;
    std::shared_ptr<small::catalog::CatalogServiceImpl> catalog_svc_;
    std::shared_ptr<small::txn::TxnServiceImpl> txn_svc_;
    std::unique_ptr<grpc::Server> grpc_server_;
};

// Fixture: ensures the in-process node is up, then creates a fresh
// users_<n> table seeded with (id=1, balance=100). Each test in a
// suite gets its own table so state doesn't leak across tests.
class TxnTestFixture : public ::testing::Test {
 protected:
    void SetUp() override {
        InProcessNode::Instance().EnsureStarted();
        unique_table_ = "users_" + std::to_string(++table_counter_);

        small::txn::Txn ddl;
        ASSERT_TRUE(ddl.Execute(
                          "CREATE TABLE " + unique_table_ +
                          " (id INT PRIMARY KEY, balance INT) "
                          "PARTITION BY LIST (id)")
                        .ok());
        ASSERT_TRUE(ddl.Execute(
                          "CREATE TABLE " + unique_table_ + "_p PARTITION OF " +
                          unique_table_ + " FOR VALUES IN (1, 2, 3)")
                        .ok());

        small::txn::Txn seed;
        ASSERT_TRUE(seed.Execute("INSERT INTO " + unique_table_ +
                                 " (id, balance) VALUES (1, 100)")
                        .ok());
    }

    std::string unique_table_;
    inline static int table_counter_ = 0;
};

}  // namespace small::test
