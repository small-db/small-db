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

#include "src/lock/lock_manager.h"

#include <memory>
#include <string>
#include <utility>

namespace small::lock {

LockManager* LockManager::GetInstance() {
    static LockManager instance;
    return &instance;
}

LockManager::Lock LockManager::Acquire(const std::string& table,
                                       const std::string& pk) {
    std::shared_ptr<std::mutex> row_mu;
    {
        std::lock_guard<std::mutex> g(map_mu_);
        RowKey key{table, pk};
        auto it = locks_.find(key);
        if (it == locks_.end()) {
            it = locks_.emplace(std::move(key), std::make_shared<std::mutex>())
                     .first;
        }
        row_mu = it->second;
    }
    // Lock the row mutex *outside* map_mu_ so waiting on a hot row does
    // not stall lookups for unrelated rows.
    return Lock(std::move(row_mu));
}

}  // namespace small::lock
