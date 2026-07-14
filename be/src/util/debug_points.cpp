// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/debug_points.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <string>
#include <string_view>

#include "common/logging.h"
#include "util/time.h"

namespace doris {
namespace {

constexpr uint64_t UNINITIALIZED_DEBUG_POINT_MODULE_MASK = UINT64_MAX;

constexpr uint64_t all_debug_point_modules_mask() {
    return (1ULL << static_cast<uint8_t>(DebugPointModule::MAX)) - 1;
}

std::atomic<uint64_t> g_enabled_debug_point_module_mask {UNINITIALIZED_DEBUG_POINT_MODULE_MASK};

std::string trim_module_name(std::string_view value) {
    auto begin = value.begin();
    auto end = value.end();
    while (begin != end && std::isspace(static_cast<unsigned char>(*begin))) {
        ++begin;
    }
    while (begin != end && std::isspace(static_cast<unsigned char>(*(end - 1)))) {
        --end;
    }
    std::string result(begin, end);
    std::ranges::transform(result, result.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return result;
}

uint64_t debug_point_module_mask(std::string_view modules) {
    uint64_t mask = 0;
    std::stringstream ss {std::string(modules)};
    std::string module;
    while (std::getline(ss, module, ',')) {
        module = trim_module_name(module);
        if (module.empty()) {
            continue;
        }
        if (module == "*" || module == "all") {
            return all_debug_point_modules_mask();
        }
        if (module == "compaction") {
            mask |= 1ULL << static_cast<uint8_t>(DebugPointModule::COMPACTION);
        } else if (module == "file_reader") {
            mask |= 1ULL << static_cast<uint8_t>(DebugPointModule::FILE_READER);
        } else if (module == "load") {
            mask |= 1ULL << static_cast<uint8_t>(DebugPointModule::LOAD);
        } else if (module == "scan") {
            mask |= 1ULL << static_cast<uint8_t>(DebugPointModule::SCAN);
        } else {
            LOG(WARNING) << "unknown debug point module: " << module;
        }
    }
    return mask;
}

config::RegisterConfUpdateCallback reg_update_debug_point_modules(
        "enable_debug_point_modules", [](const void*, const void* new_ptr) {
            DebugPoints::update_enabled_modules(*reinterpret_cast<const std::string*>(new_ptr));
        });

} // namespace

DebugPoints::DebugPoints() : _debug_points(std::make_shared<const DebugPointMap>()) {}

DebugPoints* DebugPoints::instance() {
    static DebugPoints instance;
    return &instance;
}

bool DebugPoints::is_enable(const std::string& name) {
    return get_debug_point(name) != nullptr;
}

std::shared_ptr<DebugPoint> DebugPoints::get_debug_point(const std::string& name) {
    if (!config::enable_debug_points) {
        return nullptr;
    }
    auto map_ptr = _debug_points.load();
    auto it = map_ptr->find(name);
    if (it == map_ptr->end()) {
        return nullptr;
    }

    auto debug_point = it->second;
    if ((debug_point->expire_ms > 0 && MonotonicMillis() >= debug_point->expire_ms) ||
        (debug_point->execute_limit > 0 &&
         debug_point->execute_num.fetch_add(1, std::memory_order_relaxed) >=
                 debug_point->execute_limit)) {
        remove(name);
        return nullptr;
    }

    return debug_point;
}

bool DebugPoints::is_module_enabled(DebugPointModule module) {
    if (!config::enable_debug_points) {
        return false;
    }
    auto bit = 1ULL << static_cast<uint8_t>(module);
    auto mask = g_enabled_debug_point_module_mask.load(std::memory_order_relaxed);
    if (UNLIKELY(mask == UNINITIALIZED_DEBUG_POINT_MODULE_MASK)) {
        update_enabled_modules(config::enable_debug_point_modules);
        mask = g_enabled_debug_point_module_mask.load(std::memory_order_relaxed);
    }
    return (mask & bit) != 0;
}

void DebugPoints::update_enabled_modules(const std::string& modules) {
    auto mask = debug_point_module_mask(modules);
    g_enabled_debug_point_module_mask.store(mask, std::memory_order_relaxed);
    LOG(INFO) << "set enable_debug_point_modules=" << modules << ", mask=" << mask;
}

void DebugPoints::add(const std::string& name, std::shared_ptr<DebugPoint> debug_point) {
    update([&](DebugPointMap& new_points) { new_points[name] = debug_point; });

    std::ostringstream oss;
    oss << "{";
    for (auto [key, value] : debug_point->params) {
        oss << key << " : " << value << ", ";
    }
    oss << "}";

    LOG(INFO) << "add debug point: name=" << name << ", params=" << oss.str();
}

void DebugPoints::remove(const std::string& name) {
    bool exists = false;
    update([&](DebugPointMap& new_points) { exists = new_points.erase(name) > 0; });

    LOG(INFO) << "remove debug point: name=" << name << ", exists=" << exists;
}

void DebugPoints::update(std::function<void(DebugPointMap&)>&& handler) {
    auto old_points = _debug_points.load();
    while (true) {
        auto new_points = std::make_shared<DebugPointMap>(*old_points);
        handler(*new_points);
        if (_debug_points.compare_exchange_strong(
                    old_points, std::static_pointer_cast<const DebugPointMap>(new_points))) {
            break;
        }
    }
}

void DebugPoints::clear() {
    _debug_points.store(std::make_shared<const DebugPointMap>());
    LOG(INFO) << "clear debug points";
}

} // namespace doris
