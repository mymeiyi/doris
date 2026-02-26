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

import org.apache.doris.common.Config;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.metric.CloudMetrics;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MetaServiceRateLimiter {
    private static final Logger LOG = LogManager.getLogger(MetaServiceRateLimiter.class);

    private static volatile MetaServiceRateLimiter instance;
    private volatile boolean lastEnabled = false;
    private volatile int lastMaxWaitRequestNum = 0;
    private volatile int lastDefaultQps = 0;
    private volatile String lastQpsConfig = "";
    private volatile String lastCostConfig = "";
    private Map<String, Integer> methodQpsConfig = new ConcurrentHashMap<>();
    private Map<String, Integer> methodCostConfig = new ConcurrentHashMap<>();
    private final Map<String, MethodRateLimiter> methodLimiters = new ConcurrentHashMap<>();

    public static MetaServiceRateLimiter getInstance() {
        if (instance == null) {
            synchronized (MetaServiceRateLimiter.class) {
                if (instance == null) {
                    instance = new MetaServiceRateLimiter();
                }
            }
        }
        return instance;
    }

    public MetaServiceRateLimiter() {
        reloadConfig();
    }

    private boolean isConfigChanged() {
        boolean enabled = Config.meta_service_rpc_rate_limit_enabled;
        if (enabled != lastEnabled) {
            return true;
        } else if (!enabled) {
            // If disabled, only check enabled flag
            return false;
        } else {
            return Config.meta_service_rpc_rate_limit_default_qps_per_core != lastDefaultQps
                    || Config.meta_service_rpc_rate_limit_max_wait_request_num != lastMaxWaitRequestNum
                    || !Objects.equals(Config.meta_service_rpc_rate_limit_qps_per_core_config, lastQpsConfig)
                    || !Objects.equals(Config.meta_service_rpc_cost_limit_per_core_config, lastCostConfig);
        }
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
            boolean enabled = Config.meta_service_rpc_rate_limit_enabled;
            // If disabled, clear all limiters
            if (!enabled) {
                methodLimiters.clear();
                methodQpsConfig.clear();
                methodCostConfig.clear();
                lastEnabled = enabled;
                return true;
            }
            int maxWaitRequestNum = Config.meta_service_rpc_rate_limit_max_wait_request_num;
            int defaultQpsPerCore = Config.meta_service_rpc_rate_limit_default_qps_per_core;
            String qpsConfig = Config.meta_service_rpc_rate_limit_qps_per_core_config;
            String costConfig = Config.meta_service_rpc_cost_limit_per_core_config;
            // Parse the qps and cost config
            methodQpsConfig = parseConfig(qpsConfig, "QPS");
            methodCostConfig = parseConfig(costConfig, "cost limit");
            // Update limiters
            updateMethodLimiters(defaultQpsPerCore, maxWaitRequestNum);
            // Update last config
            lastEnabled = enabled;
            lastMaxWaitRequestNum = maxWaitRequestNum;
            lastDefaultQps = defaultQpsPerCore;
            lastQpsConfig = qpsConfig;
            lastCostConfig = costConfig;
            LOG.info("Reload meta service RPC rate limit config. enabled: {}, maxWaitRequestNum: {}, "
                            + "defaultQps: {}, qps config: {}, cost config: {}", lastEnabled, lastMaxWaitRequestNum,
                    lastDefaultQps, lastQpsConfig, lastCostConfig);
        }
        return true;
    }

    private void updateMethodLimiters(int defaultQpsPerCore, int maxWaitRequestNum) {
        List<String> toRemove = new ArrayList<>();
        for (Entry<String, MethodRateLimiter> entry : methodLimiters.entrySet()) {
            String methodName = entry.getKey();
            int qps = getMethodTotalQps(methodName, defaultQpsPerCore);
            int costLimit = getMethodTotalCostLimit(methodName);
            if (qps <= 0 && costLimit <= 0) {
                toRemove.add(methodName);
                continue;
            }
            MethodRateLimiter limiter = entry.getValue();
            limiter.updateQps(qps, maxWaitRequestNum);
            limiter.updateCostLimit(costLimit);
            LOG.info("Updated rate limiter for method {}: qps={}, cost={}", methodName, qps, costLimit);
        }
        LOG.info("Removed zero QPS rate limiter for methods: {}", toRemove);
        for (String methodName : toRemove) {
            methodLimiters.remove(methodName);
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
        return qpsPerCore * getAvailableProcessors();
    }

    private int getMethodTotalCostLimit(String methodName) {
        int costPerCore = methodCostConfig.getOrDefault(methodName, 0);
        if (costPerCore <= 0) {
            return 0;
        }
        return costPerCore * getAvailableProcessors();
    }

    protected int getAvailableProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }

    private MethodRateLimiter getMethodLimiter(String methodName) {
        return methodLimiters.compute(methodName, (name, limiter) -> {
            if (limiter != null) {
                return limiter;
            }
            int qps = getMethodTotalQps(name, Config.meta_service_rpc_rate_limit_default_qps_per_core);
            int costLimit = getMethodTotalCostLimit(name);
            if (qps > 0 || costLimit > 0) {
                MethodRateLimiter newLimiter = new MethodRateLimiter(name,
                        Config.meta_service_rpc_rate_limit_max_wait_request_num, qps, costLimit);
                return newLimiter;
            }
            return null;
        });
    }

    public boolean acquire(String methodName, int cost) throws RpcRateLimitException {
        if (isConfigChanged()) {
            reloadConfig();
        }

        if (!Config.meta_service_rpc_rate_limit_enabled) {
            return false;
        }

        MethodRateLimiter limiter = getMethodLimiter(methodName);
        if (limiter != null) {
            return limiter.acquire(cost);
        }
        return false;
    }

    public void release(String methodName, int cost) {
        MethodRateLimiter limiter = methodLimiters.get(methodName);
        if (limiter != null) {
            limiter.release(cost);
        }
    }

    @VisibleForTesting
    Map<String, Integer> getMethodQpsConfig() {
        return methodQpsConfig;
    }

    @VisibleForTesting
    Map<String, Integer> getMethodCostConfig() {
        return methodCostConfig;
    }

    @VisibleForTesting
    Map<String, MethodRateLimiter> getMethodLimiters() {
        return methodLimiters;
    }

    protected static class MethodRateLimiter {
        private final String methodName;
        private volatile int maxWaitRequestNum;
        private Semaphore waitingSemaphore;
        private RateLimiter rateLimiter;
        private CostLimiter costLimiter;

        MethodRateLimiter(String methodName, int maxWaitRequestNum, int qps, int costLimit) {
            this.methodName = methodName;
            if (qps > 0) {
                this.maxWaitRequestNum = maxWaitRequestNum;
                this.waitingSemaphore = new Semaphore(maxWaitRequestNum);
                this.rateLimiter = RateLimiter.create(qps);
            }
            this.costLimiter = costLimit > 0 ? new CostLimiter(methodName, costLimit) : null;
            LOG.info("Create rate limiter for method={}, qps={}, maxWaitRequestNum={}, cost={}", methodName, qps,
                    maxWaitRequestNum, costLimit);
        }

        boolean acquire(int cost) throws RpcRateLimitException {
            long startAt = System.nanoTime();
            boolean acquired = acquireCostLimit(cost);
            try {
                acquireQpsRateLimit();
                return acquired;
            } catch (RpcRateLimitException | RuntimeException e) {
                if (acquired) {
                    try {
                        release(cost);
                    } catch (Exception releaseEx) {
                        LOG.warn("Failed to release cost reservation for method {} after QPS limit failure",
                                methodName, releaseEx);
                    }
                }
                throw e;
            } finally {
                long durationNs = System.nanoTime() - startAt;
                SummaryProfile summaryProfile = SummaryProfile.getSummaryProfile(ConnectContext.get());
                if (summaryProfile != null) {
                    summaryProfile.addWaitMsRpcRateLimiterTime(durationNs);
                }
                if (MetricRepo.isInit && Config.isCloudMode()) {
                    CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED_LATENCY.getOrAdd(methodName)
                            .update(TimeUnit.NANOSECONDS.toMillis(durationNs));
                }
            }
        }

        boolean acquireCostLimit(int cost) throws RpcRateLimitException {
            if (costLimiter == null || cost <= 0) {
                return false;
            }
            boolean acquired = false;
            try {
                acquired = costLimiter.acquire(cost, Config.meta_service_rpc_rate_limit_wait_timeout_ms,
                        TimeUnit.MILLISECONDS);
                if (!acquired) {
                    throw new RpcRateLimitException(
                            "Meta service RPC rate limit timeout while waiting for cost limit for method: "
                                    + methodName + ", cost: " + cost);
                }
            } catch (InterruptedException e) {
                throw new RpcRateLimitException(
                        "Meta service RPC rate limit interrupted while waiting for cost limit for method: "
                                + methodName, e);
            } finally {
                if (MetricRepo.isInit && Config.isCloudMode()) {
                    if (!acquired) {
                        CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED.getOrAdd(methodName).increase(1L);
                    }
                }
            }
            return acquired;
        }

        private void acquireQpsRateLimit() throws RpcRateLimitException {
            if (rateLimiter == null || waitingSemaphore == null) {
                return;
            }
            if (!waitingSemaphore.tryAcquire()) {
                if (MetricRepo.isInit && Config.isCloudMode()) {
                    CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED.getOrAdd(methodName).increase(1L);
                }
                throw new RpcRateLimitException(
                    "Meta service RPC rate limit exceeded for method: " + methodName
                    + ", too many waiting requests (max=" + maxWaitRequestNum + ")");
            }

            try {
                long timeoutMs = Config.meta_service_rpc_rate_limit_wait_timeout_ms;
                boolean acquired = rateLimiter.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);

                if (!acquired) {
                    if (MetricRepo.isInit && Config.isCloudMode()) {
                        CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED.getOrAdd(methodName).increase(1L);
                    }
                    throw new RpcRateLimitException(
                        "Meta service RPC rate limit timeout for method: " + methodName
                        + ", waited " + timeoutMs + "ms");
                }
            } catch (RpcRateLimitException e) {
                throw e;
            } catch (Exception e) {
                throw new RpcRateLimitException(
                    "Failed to acquire rate limit for method: " + methodName, e);
            } finally {
                waitingSemaphore.release();
            }
        }

        void release(int cost) {
            if (costLimiter != null && cost > 0) {
                costLimiter.release(cost);
            }
        }

        void updateQps(int qps, int maxWaitRequestNum) {
            if (qps <= 0) {
                rateLimiter = null;
                waitingSemaphore = null;
                return;
            }
            if (this.waitingSemaphore == null || maxWaitRequestNum != this.maxWaitRequestNum) {
                this.maxWaitRequestNum = maxWaitRequestNum;
                this.waitingSemaphore = new Semaphore(maxWaitRequestNum);
            }
            if (rateLimiter == null) {
                rateLimiter = RateLimiter.create(qps);
            } else if (qps != rateLimiter.getRate()) {
                rateLimiter.setRate(qps);
            }
            LOG.info("Updated rate limiter for method {}: qps={}", methodName, qps);
        }

        void updateCostLimit(int costLimit) {
            if (costLimit <= 0) {
                costLimiter = null;
                return;
            }
            if (costLimiter == null) {
                costLimiter = new CostLimiter(methodName, costLimit);
            } else {
                costLimiter.setLimit(costLimit);
            }
        }

        @VisibleForTesting
        RateLimiter getRateLimiter() {
            return rateLimiter;
        }

        @VisibleForTesting
        int getAllowWaiting() {
            return waitingSemaphore != null ? waitingSemaphore.availablePermits() : -1;
        }

        @VisibleForTesting
        CostLimiter getCostLimiter() {
            return costLimiter;
        }
    }

    protected static class CostLimiter {
        private final String methodName;
        private volatile int limit;
        private int currentCost;
        private final Lock lock = new ReentrantLock();
        private final Condition condition = lock.newCondition();

        CostLimiter(String methodName, int limit) {
            if (limit < 0) {
                throw new IllegalArgumentException("limit must be >= 0");
            }
            this.methodName = methodName;
            this.limit = limit;
            this.currentCost = 0;
        }

        // modify limit to newLimit (thread safe)
        void setLimit(int newLimit) {
            if (newLimit < 0) {
                throw new IllegalArgumentException("newLimit must be >= 0");
            }
            lock.lock();
            try {
                this.limit = newLimit;
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }

        boolean acquire(int cost, long timeout, TimeUnit unit) throws InterruptedException, RpcRateLimitException {
            if (cost > limit) {
                throw new CostExceedLimitException("Cost " + cost + " exceeds the limit " + limit);
            }
            long nanos = unit.toNanos(timeout);
            lock.lockInterruptibly();
            try {
                while (currentCost + cost > limit) {
                    if (nanos <= 0) {
                        return false;
                    }
                    nanos = condition.awaitNanos(nanos);
                }
                currentCost += cost;
                return true;
            } finally {
                lock.unlock();
            }
        }

        void release(int cost) {
            lock.lock();
            try {
                currentCost -= cost;
                if (currentCost < 0) {
                    currentCost = 0;
                }
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }

        @VisibleForTesting
        int getCurrentCost() {
            lock.lock();
            try {
                return currentCost;
            } finally {
                lock.unlock();
            }
        }
    }
}
