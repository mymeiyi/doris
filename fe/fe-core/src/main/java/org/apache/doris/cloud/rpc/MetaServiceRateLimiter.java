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
import org.apache.doris.metric.CloudMetrics;
import org.apache.doris.metric.MetricRepo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
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
    private volatile int lastDefaultQps = 0;
    private volatile int lastMaxWaiting = 0;
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
                    || Config.meta_service_rpc_rate_limit_max_waiting != lastMaxWaiting
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
            // Parse the QPS config
            int defaultQpsPerCore = Config.meta_service_rpc_rate_limit_default_qps_per_core;
            int maxWaiting = Config.meta_service_rpc_rate_limit_max_waiting;
            String qpsConfig = Config.meta_service_rpc_rate_limit_qps_per_core_config;
            String costConfig = Config.meta_service_rpc_cost_limit_per_core_config;
            methodQpsConfig = parseConfig(qpsConfig, "QPS");
            methodCostConfig = parseConfig(costConfig, "cost limit");
            // Update limiters
            updateMethodLimiters(defaultQpsPerCore, maxWaiting);
            // Update last config
            lastEnabled = enabled;
            lastMaxWaiting = maxWaiting;
            lastDefaultQps = defaultQpsPerCore;
            lastQpsConfig = qpsConfig;
            lastCostConfig = costConfig;
            LOG.info(
                    "Reloaded meta service RPC rate limit enabled: {}, maxWaiting: {}, defaultQps: {}, qps config: {}, cost config: {}",
                    lastEnabled, lastMaxWaiting, lastDefaultQps, lastQpsConfig, lastCostConfig);
        }
        return true;
    }

    private void updateMethodLimiters(int defaultQpsPerCore, int maxWaiting) {
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
            limiter.updateQps(qps, maxWaiting);
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
            return Collections.emptyMap();
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

    // TODO mock
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
                        Config.meta_service_rpc_rate_limit_max_waiting, qps, costLimit);
                return newLimiter;
            }
            return null;
        });
    }

    public void acquire(String methodName, int cost) throws RpcRateLimitException {
        if (isConfigChanged()) {
            reloadConfig();
        }

        if (!Config.meta_service_rpc_rate_limit_enabled) {
            return;
        }

        MethodRateLimiter limiter = getMethodLimiter(methodName);
        if (limiter != null) {
            limiter.acquire(cost);
        }
    }

    public void release(String methodName, int cost) {
        if (!Config.meta_service_rpc_rate_limit_enabled) {
            return;
        }

        MethodRateLimiter limiter = methodLimiters.get(methodName);
        if (limiter != null) {
            limiter.release(cost);
        }
    }

    @VisibleForTesting
    public Map<String, Integer> getMethodQpsConfig() {
        return methodQpsConfig;
    }

    @VisibleForTesting
    public Map<String, Integer> getMethodCostConfig() {
        return methodCostConfig;
    }

    @VisibleForTesting
    public Map<String, MethodRateLimiter> getMethodLimiters() {
        return methodLimiters;
    }

    protected static class MethodRateLimiter {
        private final String methodName;
        private volatile int maxWaiting;
        private Semaphore waitingSemaphore;
        private RateLimiter rateLimiter;
        private CostLimiter costLimiter;

        MethodRateLimiter(String methodName, int maxWaiting, int qps, int costLimit) {
            this.methodName = methodName;
            if (qps > 0) {
                this.maxWaiting = maxWaiting;
                this.waitingSemaphore = new Semaphore(maxWaiting);
                this.rateLimiter = RateLimiter.create(qps);
            }
            this.costLimiter = costLimit > 0 ? new CostLimiter(costLimit) : null;
            LOG.info("Create rate limiter for method={}, qps={}, maxWaiting={}, cost={}", methodName, qps, maxWaiting,
                    costLimit);
        }

        void acquire(int cost) throws RpcRateLimitException {
            acquireCostLimit(cost);
            acquireQpsRateLimit();
        }

        void acquireCostLimit(int cost) throws RpcRateLimitException {
            if (costLimiter == null || cost <= 0) {
                return;
            }
            boolean acquired = false;
            try {
                acquired = costLimiter.acquire(cost, Config.meta_service_rpc_rate_limit_wait_timeout_ms,
                        TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RpcRateLimitException(
                        "Meta service RPC rate limit interrupted while waiting for cost limit for method: "
                                + methodName, e);
            } finally {
                if (!acquired) {
                    throw new RpcRateLimitException(
                            "Meta service RPC rate limit timeout while waiting for cost limit for method: "
                                    + methodName + ", cost: " + cost);
                }
            }
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
                    + ", too many waiting requests (max=" + maxWaiting + ")");
            }

            long startTime = System.currentTimeMillis();
            try {
                long timeoutMs = Config.meta_service_rpc_rate_limit_wait_timeout_ms;
                boolean acquired = rateLimiter.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);

                if (!acquired) {
                    // release();
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
                // release();
                throw new RpcRateLimitException(
                    "Failed to acquire rate limit for method: " + methodName, e);
            } finally {
                waitingSemaphore.release();
                if (MetricRepo.isInit && Config.isCloudMode()) {
                    CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED_LATENCY.getOrAdd(methodName)
                            .update(System.currentTimeMillis() - startTime);
                }
            }
        }

        void release(int cost) {
            if (costLimiter != null && cost > 0) {
                costLimiter.release(cost);
            }
        }

        void updateQps(int qps, int maxWaiting) {
            if (qps <= 0) {
                rateLimiter = null;
                waitingSemaphore = null;
                return;
            }
            if (maxWaiting != this.maxWaiting) {
                this.maxWaiting = maxWaiting;
                this.waitingSemaphore = new Semaphore(maxWaiting);
            }
            if (qps != rateLimiter.getRate()) {
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
                costLimiter = new CostLimiter(costLimit);
            } else {
                costLimiter.setLimit(costLimit);
            }
        }

        @VisibleForTesting
        RateLimiter getRateLimiter() {
            return rateLimiter;
        }

        @VisibleForTesting
        public int getAllowWaiting() {
            return waitingSemaphore != null ? waitingSemaphore.availablePermits() : -1;
        }
    }

    protected static class CostLimiter {
        private volatile int limit;
        private int currentCost;
        private final Lock lock = new ReentrantLock();
        private final Condition condition = lock.newCondition();

        public CostLimiter(int limit) {
            if (limit < 0) {
                throw new IllegalArgumentException("limit must be >= 0");
            }
            this.limit = limit;
            this.currentCost = 0;
        }

        /**
         * 动态修改总代价上限（线程安全）
         * @param newLimit 新的上限值，必须 >= 0
         */
        public void setLimit(int newLimit) {
            if (newLimit < 0) {
                throw new IllegalArgumentException("newLimit must be >= 0");
            }
            lock.lock();
            try {
                this.limit = newLimit;
                // 无论上限增加还是减少，都唤醒所有等待线程，让它们重新检查条件
                // 如果上限增加，部分线程可能现在可以获取令牌；如果上限减少，等待线程会继续等待
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }

        public int getLimit() {
            // limit 本身由锁保护，但为了无锁读取可以用 volatile（此处直接返回，因有锁可见性保证）
            lock.lock();
            try {
                return limit;
            } finally {
                lock.unlock();
            }
        }

        public int getCurrentCost() {
            lock.lock();
            try {
                return currentCost;
            } finally {
                lock.unlock();
            }
        }

        /**
         * 尝试获取代价，最多等待指定时间
         */
        public boolean acquire(int cost, long timeout, TimeUnit unit) throws InterruptedException {
            if (cost < 0) {
                throw new IllegalArgumentException("cost must be >= 0");
            }
            if (cost > limit) {
                return false;  // 单个代价超过上限直接拒绝
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

        public void release(int cost) {
            if (cost < 0) {
                throw new IllegalArgumentException("cost must be >= 0");
            }
            lock.lock();
            try {
                currentCost -= cost;
                if (currentCost < 0) currentCost = 0;  // 防御性清零
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }

        public boolean tryAcquire(int cost) {
            if (cost < 0) {
                throw new IllegalArgumentException("cost must be >= 0");
            }
            lock.lock();
            try {
                if (currentCost + cost <= limit) {
                    currentCost += cost;
                    return true;
                }
                return false;
            } finally {
                lock.unlock();
            }
        }

        public void acquire(int cost) throws InterruptedException {
            if (cost < 0) {
                throw new IllegalArgumentException("cost must be >= 0");
            }
            lock.lockInterruptibly();
            try {
                while (currentCost + cost > limit) {
                    condition.await();
                }
                currentCost += cost;
            } finally {
                lock.unlock();
            }
        }
    }
}
