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

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>

namespace small::lock {

// Composite key for the per-row lock map: (table_name, primary_key).
using RowKey = std::tuple<std::string, std::string>;

struct RowKeyHash {
    size_t operator()(const RowKey& k) const {
        size_t h1 = std::hash<std::string>{}(std::get<0>(k));
        size_t h2 = std::hash<std::string>{}(std::get<1>(k));
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
    }
};

// Per-process exclusive row-lock manager. Exclusive locks only;
// readers go through MVCC and don't acquire any.
class LockManager {
   public:
    static LockManager* GetInstance();

    LockManager(const LockManager&) = delete;
    LockManager& operator=(const LockManager&) = delete;

    // RAII handle. Constructor acquires the per-row mutex; destructor
    // releases it. Move-only.
    class Lock {
       public:
        Lock() = default;
        explicit Lock(std::shared_ptr<std::mutex> m) : m_(std::move(m)) {
            if (m_) m_->lock();
        }
        ~Lock() {
            if (m_) m_->unlock();
        }
        Lock(Lock&& other) noexcept : m_(std::move(other.m_)) {
            other.m_.reset();
        }
        Lock& operator=(Lock&& other) noexcept {
            if (this != &other) {
                if (m_) m_->unlock();
                m_ = std::move(other.m_);
                other.m_.reset();
            }
            return *this;
        }
        Lock(const Lock&) = delete;
        Lock& operator=(const Lock&) = delete;

       private:
        std::shared_ptr<std::mutex> m_;
    };

    Lock Acquire(const std::string& table, const std::string& pk);

   private:
    LockManager() = default;

    std::mutex map_mu_;
    std::unordered_map<RowKey, std::shared_ptr<std::mutex>, RowKeyHash> locks_;
};

}  // namespace small::lock
