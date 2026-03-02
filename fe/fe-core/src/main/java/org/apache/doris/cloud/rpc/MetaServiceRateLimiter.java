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

package org.apache.doris.cloud.rpc;

import org.apache.doris.cloud.rpc.RpcRateLimiter.BackpressureQpsLimiter;
import org.apache.doris.cloud.rpc.RpcRateLimiter.CostLimiter;
import org.apache.doris.cloud.rpc.RpcRateLimiter.QpsLimiter;
import org.apache.doris.common.Config;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MetaServiceRateLimiter {
    private static final Logger LOG = LogManager.getLogger(MetaServiceRateLimiter.class);

    private static int processorCount;
    private static volatile MetaServiceRateLimiter instance;
    private volatile boolean lastEnabled = false;
    private volatile int lastMaxWaitRequestNum = 0;
    private volatile int lastDefaultQps = 0;
    private volatile String lastQpsConfig = "";
    private volatile String lastCostConfig = "";
    private volatile boolean lastAdaptiveThrottleEnabled = false;
    private volatile String lastAdaptiveThrottleMethods = "";
    private Map<String, Integer> methodQpsConfig = new ConcurrentHashMap<>();
    private Map<String, Integer> methodCostConfig = new ConcurrentHashMap<>();
    private Set<String> adaptiveThrottleMethods = ConcurrentHashMap.newKeySet();

    private final Map<String, QpsLimiter> qpsLimiters = new ConcurrentHashMap<>();
    private final Map<String, CostLimiter> costLimiters = new ConcurrentHashMap<>();
    private final Map<String, BackpressureQpsLimiter> backpressureQpsLimiters = new ConcurrentHashMap<>();

    public static MetaServiceRateLimiter getInstance() {
        if (instance == null) {
            synchronized (MetaServiceRateLimiter.class) {
                if (instance == null) {
                    instance = new MetaServiceRateLimiter(Runtime.getRuntime().availableProcessors());
                }
            }
        }
        return instance;
    }

    MetaServiceRateLimiter(int processorCount) {
        this.processorCount = processorCount;
        reloadConfig();
        /*if (Config.meta_service_rpc_adaptive_throttle_enabled) {
            MetaServiceAdaptiveThrottle.getInstance().setFactorChangeListener(this::setAdaptiveFactor);
        }*/
    }

    @VisibleForTesting
    boolean isConfigChanged() {
        return Config.meta_service_rpc_rate_limit_enabled != lastEnabled
                || Config.meta_service_rpc_rate_limit_default_qps_per_core != lastDefaultQps
                || Config.meta_service_rpc_rate_limit_max_waiting_request_num != lastMaxWaitRequestNum
                || !Objects.equals(Config.meta_service_rpc_rate_limit_qps_per_core_config, lastQpsConfig)
                || !Objects.equals(Config.meta_service_rpc_cost_limit_per_core_config, lastCostConfig)
                || Config.meta_service_rpc_adaptive_throttle_enabled != lastAdaptiveThrottleEnabled
                || !Objects.equals(Config.meta_service_rpc_adaptive_throttle_methods,
                lastAdaptiveThrottleMethods);
        /*boolean enabled = Config.meta_service_rpc_rate_limit_enabled;
        boolean adaptiveThrottleEnabled = Config.meta_service_rpc_adaptive_throttle_enabled;

        if (enabled != lastEnabled || adaptiveThrottleEnabled != lastAdaptiveThrottleEnabled) {
            return true;
        } else if (!enabled && !adaptiveThrottleEnabled) {
            // If disabled, only check enabled flags
            return false;
        } else {
            return Config.meta_service_rpc_rate_limit_default_qps_per_core != lastDefaultQps
                    || Config.meta_service_rpc_rate_limit_max_waiting_request_num != lastMaxWaitRequestNum
                    || !Objects.equals(Config.meta_service_rpc_rate_limit_qps_per_core_config, lastQpsConfig)
                    || !Objects.equals(Config.meta_service_rpc_cost_limit_per_core_config, lastCostConfig)
                    || !Objects.equals(Config.meta_service_rpc_adaptive_throttle_methods,
                    lastAdaptiveThrottleMethods);
        }*/
    }

    @VisibleForTesting
    protected boolean reloadConfig() {
        if (!isConfigChanged()) {
            return false;
        }
        synchronized (this) {
            if (!isConfigChanged()) {
                return false;
            }
            reloadRateLimiterConfig();
            reloadAdaptiveThrottleConfig();
        }
        return true;
    }

    private void reloadRateLimiterConfig() {
        boolean enabled = Config.meta_service_rpc_rate_limit_enabled;
        int maxWaitRequestNum = Config.meta_service_rpc_rate_limit_max_waiting_request_num;
        int defaultQpsPerCore = Config.meta_service_rpc_rate_limit_default_qps_per_core;
        String qpsConfig = Config.meta_service_rpc_rate_limit_qps_per_core_config;
        String costConfig = Config.meta_service_rpc_cost_limit_per_core_config;
        // Parse the qps and cost config
        methodQpsConfig = parseConfig(qpsConfig, "QPS");
        methodCostConfig = parseConfig(costConfig, "cost limit");

        // If disabled, clear all limiters
        if (!enabled) {
            methodQpsConfig.clear();
            methodCostConfig.clear();
            qpsLimiters.clear();
            costLimiters.clear();
        } else {
            // Update limiters
            updateQpsLimiters(defaultQpsPerCore, maxWaitRequestNum);
            updateCostLimiters();
        }
        // Update last config
        lastEnabled = enabled;
        lastMaxWaitRequestNum = maxWaitRequestNum;
        lastDefaultQps = defaultQpsPerCore;
        lastQpsConfig = qpsConfig;
        lastCostConfig = costConfig;
        LOG.info("Reload meta service rpc rate limit config. enabled: {}, maxWaitRequestNum: {}, "
                        + "defaultQps: {}, qpsConfig: [{}], costConfig: [{}]", lastEnabled, lastMaxWaitRequestNum,
                lastDefaultQps, lastQpsConfig, lastCostConfig);
    }

    private void reloadAdaptiveThrottleConfig() {
        boolean adaptiveThrottleEnabled = Config.meta_service_rpc_adaptive_throttle_enabled;
        String adaptiveThrottleMethods = Config.meta_service_rpc_adaptive_throttle_methods;
        if (!adaptiveThrottleEnabled) {
            this.adaptiveThrottleMethods.clear();
            this.backpressureQpsLimiters.clear();
        } else {
            Set<String> newAdaptiveThrottleMethods = new HashSet<>();
            if (adaptiveThrottleMethods != null && !adaptiveThrottleMethods.isEmpty()) {
                for (String method : adaptiveThrottleMethods.split(",")) {
                    String trimmed = method.trim();
                    if (!trimmed.isEmpty()) {
                        newAdaptiveThrottleMethods.add(trimmed);
                    }
                }
            }
            this.adaptiveThrottleMethods.clear();
            this.adaptiveThrottleMethods.addAll(newAdaptiveThrottleMethods);
        }
        lastAdaptiveThrottleEnabled = adaptiveThrottleEnabled;
        lastAdaptiveThrottleMethods = adaptiveThrottleMethods;
    }

    private void updateQpsLimiters(int defaultQpsPerCore, int maxWaitRequestNum) {
        List<String> toRemove = new ArrayList<>();
        for (Entry<String, QpsLimiter> entry : qpsLimiters.entrySet()) {
            String methodName = entry.getKey();
            int qps = getMethodTotalQps(methodName, defaultQpsPerCore);
            if (qps <= 0) {
                toRemove.add(methodName);
                continue;
            }
            QpsLimiter limiter = entry.getValue();
            limiter.update(maxWaitRequestNum, qps);
            LOG.info("Updated rate limiter for method: {}, maxWaitRequestNum: {}, qps: {}", methodName,
                    maxWaitRequestNum, qps);
        }
        if (!toRemove.isEmpty()) {
            LOG.info("Remove zero qps rate limiter for methods: {}", toRemove);
            for (String methodName : toRemove) {
                qpsLimiters.remove(methodName);
            }
        }
    }

    private void updateCostLimiters() {
        List<String> toRemove = new ArrayList<>();
        for (Entry<String, CostLimiter> entry : costLimiters.entrySet()) {
            String methodName = entry.getKey();
            int costLimit = getMethodTotalCostLimit(methodName);
            if (costLimit <= 0) {
                toRemove.add(methodName);
                continue;
            }
            CostLimiter limiter = entry.getValue();
            limiter.setLimit(costLimit);
            LOG.info("Updated cost limiter for method: {}, cost: {}", methodName, costLimit);
        }
        if (!toRemove.isEmpty()) {
            LOG.info("Remove cost limiter for methods: {}", toRemove);
            for (String methodName : toRemove) {
                costLimiters.remove(methodName);
            }
        }
    }

    private Map<String, Integer> parseConfig(String config, String configName) {
        if (config == null || config.isEmpty()) {
            return new HashMap<>(0);
        }

        Map<String, Integer> target = new HashMap<>();
        String[] entries = config.split(";");
        for (String entry : entries) {
            if (entry.trim().isEmpty()) {
                continue;
            }
            String[] parts = entry.trim().split(":");
            if (parts.length == 2) {
                try {
                    String methodName = parts[0].trim();
                    int limit = Integer.parseInt(parts[1].trim());
                    target.put(methodName, limit);
                } catch (NumberFormatException e) {
                    LOG.warn("Invalid {} config entry: {}", configName, entry);
                }
            } else {
                LOG.warn("Invalid {} config entry: {}", configName, entry);
            }
        }
        return target;
    }

    private int getMethodTotalQps(String methodName, int defaultQpsPerCore) {
        int qpsPerCore = methodQpsConfig.getOrDefault(methodName, defaultQpsPerCore);
        if (qpsPerCore <= 0) {
            return 0;
        }
        return qpsPerCore * processorCount;
    }

    protected int getClampedCost(String methodName, int cost) {
        if (Config.meta_service_rpc_cost_clamped_to_limit_enabled) {
            int limit = getInstance().getMethodTotalCostLimit(methodName);
            if (limit > 0 && cost > limit) {
                LOG.info("Clamped cost: {} for method: {} to limit: {}", cost,
                        methodName, limit);
                cost = limit;
            }
        }
        return cost;
    }

    private int getMethodTotalCostLimit(String methodName) {
        int costPerCore = methodCostConfig.getOrDefault(methodName, 0);
        if (costPerCore <= 0) {
            return 0;
        }
        return costPerCore * processorCount;
    }

    private QpsLimiter getQpsLimiter(String methodName) {
        return qpsLimiters.compute(methodName, (name, limiter) -> {
            if (limiter != null) {
                return limiter;
            }
            int qps = getMethodTotalQps(name, Config.meta_service_rpc_rate_limit_default_qps_per_core);
            if (qps > 0) {
                return new QpsLimiter(name, Config.meta_service_rpc_rate_limit_max_waiting_request_num, qps);
            }
            return null;
        });
    }

    private CostLimiter getCostLimiter(String methodName) {
        return costLimiters.compute(methodName, (name, limiter) -> {
            if (limiter != null) {
                return limiter;
            }
            int costLimit = getMethodTotalCostLimit(name);
            if (costLimit > 0) {
                return new CostLimiter(methodName, costLimit);
            }
            return null;
        });
    }

    private BackpressureQpsLimiter getBackpressureQpsLimiter(String methodName, double factor) {
        return backpressureQpsLimiters.compute(methodName, (name, limiter) -> {
            if (limiter != null) {
                return limiter;
            }
            if (!adaptiveThrottleMethods.contains(name)) {
                return null;
            }
            int qps = getMethodTotalQps(name, Config.meta_service_rpc_rate_limit_default_qps_per_core);
            if (qps > 0) {
                return new BackpressureQpsLimiter(name, Config.meta_service_rpc_rate_limit_max_waiting_request_num, qps,
                        factor);
            }
            return null;
        });
    }

    public boolean acquire(String methodName, int cost) throws RpcRateLimitException {
        if (isConfigChanged()) {
            reloadConfig();
        }

        // Step1: Check backpressure limiter first (if adaptive throttle is active with factor < 1.0)
        if (Config.meta_service_rpc_adaptive_throttle_enabled) {
            double factor = MetaServiceAdaptiveThrottle.getInstance().getFactor();
            if (factor < 1.0) {
                QpsLimiter backpressureLimiter = getBackpressureQpsLimiter(methodName, factor);
                if (backpressureLimiter != null) {
                    backpressureLimiter.acquire();
                }
            }
        }

        if (Config.meta_service_rpc_rate_limit_enabled) {
            // Step2: Check qps limiter
            QpsLimiter qpsLimiter = getQpsLimiter(methodName);
            if (qpsLimiter != null) {
                qpsLimiter.acquire();
            }

            // Step3: Check cost limiter
            CostLimiter costLimiter = getCostLimiter(methodName);
            if (costLimiter != null && cost > 0) {
                costLimiter.acquire(cost);
                return true;
            }
        }
        return false;
    }

    public void release(String methodName, int cost) {
        CostLimiter limiter = costLimiters.get(methodName);
        if (limiter != null) {
            try {
                limiter.release(cost);
            } catch (Exception e) {
                LOG.warn("Failed to release cost limiter for method: {}, cost: {}", methodName, cost, e);
            }
        }
    }

    // only used for testing
    Map<String, Integer> getMethodQpsConfig() {
        return methodQpsConfig;
    }

    // only used for testing
    Map<String, Integer> getMethodCostConfig() {
        return methodCostConfig;
    }

    // only used for testing
    Map<String, QpsLimiter> getQpsLimiters() {
        return qpsLimiters;
    }

    // only used for testing
    Map<String, CostLimiter> getCostLimiters() {
        return costLimiters;
    }

    // only used for testing
    Map<String, BackpressureQpsLimiter> getBackpressureQpsLimiters() {
        return backpressureQpsLimiters;
    }
}
