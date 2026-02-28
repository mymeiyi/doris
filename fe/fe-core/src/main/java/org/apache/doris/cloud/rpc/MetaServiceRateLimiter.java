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

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.RpcRateLimiter.BackpressureQpsRateLimiter;
import org.apache.doris.cloud.rpc.RpcRateLimiter.CostLimiter;
import org.apache.doris.cloud.rpc.RpcRateLimiter.QpsRateLimiter;
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
import java.util.Set;
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
    // private final Map<String, MethodRateLimiter> methodLimiters = new ConcurrentHashMap<>();

    // Separate map for backpressure throttling - independent of active rate limiting
    // private final Map<String, BackpressureMethodRateLimiter> backpressureMethodLimiters = new ConcurrentHashMap<>();
    // Track all methods that should be considered for adaptive throttle (from config + observed)
    private final Set<String> adaptiveThrottleMethods = ConcurrentHashMap.newKeySet();

    private final Map<String, QpsRateLimiter> qpsLimiters = new ConcurrentHashMap<>();
    private final Map<String, CostLimiter> costLimiters = new ConcurrentHashMap<>();
    private final Map<String, BackpressureQpsRateLimiter> backpressureLimiters = new ConcurrentHashMap<>();

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

    MetaServiceRateLimiter() {
        reloadConfig();
        if (Config.meta_service_rpc_adaptive_throttle_enabled) {
            MetaServiceAdaptiveThrottle.getInstance().setFactorChangeListener(this::setAdaptiveFactor);
        }
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
                    || Config.meta_service_rpc_rate_limit_max_waiting_request_num != lastMaxWaitRequestNum
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
                costLimiters.clear();
                qpsLimiters.clear();
                // methodLimiters.clear();
                // backpressureMethodLimiters.clear();
                methodQpsConfig.clear();
                methodCostConfig.clear();
                lastEnabled = enabled;
                return true;
            }
            int maxWaitRequestNum = Config.meta_service_rpc_rate_limit_max_waiting_request_num;
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
            LOG.info("Reload meta service rpc rate limit config. enabled: {}, maxWaitRequestNum: {}, "
                            + "defaultQps: {}, qpsConfig: [{}], costConfig: [{}]", lastEnabled, lastMaxWaitRequestNum,
                    lastDefaultQps, lastQpsConfig, lastCostConfig);
        }
        return true;
    }

    private void updateMethodLimiters(int defaultQpsPerCore, int maxWaitRequestNum) {
        List<String> toRemove = new ArrayList<>();
        for (Entry<String, QpsRateLimiter> entry : qpsLimiters.entrySet()) {
            String methodName = entry.getKey();
            int qps = getMethodTotalQps(methodName, defaultQpsPerCore);
            // int costLimit = getMethodTotalCostLimit(methodName);
            if (qps <= 0 /*&& costLimit <= 0*/) {
                toRemove.add(methodName);
                continue;
            }
            QpsRateLimiter limiter = entry.getValue();
            limiter.updateQps(maxWaitRequestNum, qps);
            LOG.info("Updated rate limiter for method: {}, maxWaitRequestNum: {}, qps: {}, cost: {}", methodName,
                    maxWaitRequestNum, qps);
        }
        if (!toRemove.isEmpty()) {
            LOG.info("Remove zero qps rate limiter for methods: {}", toRemove);
            for (String methodName : toRemove) {
                qpsLimiters.remove(methodName);
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
        return qpsPerCore * getAvailableProcessors();
    }

    private int getMethodTotalCostLimit(String methodName) {
        int costPerCore = methodCostConfig.getOrDefault(methodName, 0);
        if (costPerCore <= 0) {
            return 0;
        }
        return costPerCore * getAvailableProcessors();
    }

    @VisibleForTesting
    protected int getAvailableProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }

    private CostLimiter getCostLimiter(String methodName) {
        return costLimiters.compute(methodName, (name, limiter) -> {
            if (limiter != null) {
                return limiter;
            }
            // int qps = getMethodTotalQps(name, Config.meta_service_rpc_rate_limit_default_qps_per_core);
            int costLimit = getMethodTotalCostLimit(name);
            if (/*qps > 0 || */costLimit > 0) {
                CostLimiter newLimiter = new CostLimiter(methodName, costLimit);
                return newLimiter;
            }
            return null;
        });
    }

    private QpsRateLimiter getMethodLimiter(String methodName) {
        return qpsLimiters.compute(methodName, (name, limiter) -> {
            if (limiter != null) {
                return limiter;
            }
            int qps = getMethodTotalQps(name, Config.meta_service_rpc_rate_limit_default_qps_per_core);
            // int costLimit = getMethodTotalCostLimit(name);
            if (qps > 0 /*|| costLimit > 0*/) {
                QpsRateLimiter newLimiter = new QpsRateLimiter(name,
                        Config.meta_service_rpc_rate_limit_max_waiting_request_num, qps);
                return newLimiter;
            }
            return null;
        });
    }

    private BackpressureQpsRateLimiter getBackpressureMethodLimiter(String methodName, double factor) {
        return backpressureLimiters.compute(methodName, (name, limiter) -> {
            if (limiter != null) {
                return limiter;
            }
            int qps = getMethodTotalQps(name, Config.meta_service_rpc_rate_limit_default_qps_per_core);
            int costLimit = getMethodTotalCostLimit(name);
            if (qps > 0 || costLimit > 0) {
                BackpressureQpsRateLimiter newLimiter = new BackpressureQpsRateLimiter(name, qps,
                        Config.meta_service_rpc_rate_limit_max_waiting_request_num, factor);
                return newLimiter;
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
                QpsRateLimiter backpressureLimiter = getBackpressureMethodLimiter(methodName, factor);
                if (backpressureLimiter != null) {
                    backpressureLimiter.acquire();
                }
            }
        }

        if (!Config.meta_service_rpc_rate_limit_enabled) {
            return false;
        }

        // Step2: Check qps limiter
        QpsRateLimiter qpsRateLimiter = getMethodLimiter(methodName);
        if (qpsRateLimiter != null) {
            qpsRateLimiter.acquire();
        }

        // Step3 Check cost limiter
        boolean acquired = false;
        CostLimiter costLimiter = getCostLimiter(methodName);
        if (costLimiter != null) {
            acquired = costLimiter.acquire(cost);
        }
        return acquired;
    }

    /*public boolean acquire(String methodName, int cost) throws RpcRateLimitException {
        if (isConfigChanged()) {
            reloadConfig();
        }

        // Track this method for adaptive throttle
        *//*if (Config.meta_service_rpc_adaptive_throttle_enabled) {
            adaptiveThrottleMethods.add(methodName);
        }*//*
        // Step 1: Check backpressure limiter first (if adaptive throttle is active with factor < 1.0)
        if (Config.meta_service_rpc_adaptive_throttle_enabled) {
            double factor = MetaServiceAdaptiveThrottle.getInstance().getFactor();
            if (factor < 1.0) {
                BackpressureMethodRateLimiter backpressureLimiter = getBackpressureMethodLimiter(methodName, factor);
                // BackpressureMethodRateLimiter backpressureLimiter = backpressureMethodLimiters.get(methodName);
                if (backpressureLimiter != null) {
                    try {
                        // Try to acquire backpressure limiter
                        if (!backpressureLimiter.acquire()) {
                            // Backpressure throttling triggered - return failure immediately
                            LOG.debug("Backpressure throttle triggered for method: {}", methodName);
                            return false;
                        }
                    } catch (RpcRateLimitException e) {
                        // Backpressure throttling triggered - return failure
                        LOG.debug("Backpressure throttle exception for method: {}", methodName);
                        return false;
                    }
                }
            }
        }

        // Step 2: Check active rate limiter
        if (!Config.meta_service_rpc_rate_limit_enabled) {
            return false;
        }
        MethodRateLimiter limiter = getMethodLimiter(methodName);
        if (limiter == null) {
            return false;
        }
        return limiter.acquire(cost);
    }*/

    public void release(String methodName, int cost) {
        CostLimiter limiter = costLimiters.get(methodName);
        if (limiter != null) {
            try {
                limiter.release(cost);
            } catch (Exception e) {
                LOG.warn("Failed to release rate limiter for method: {}, cost: {}", methodName, cost, e);
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
    /*Map<String, MethodRateLimiter> getMethodLimiters() {
        return methodLimiters;
    }*/

    // Get backpressure method limiters - for testing
    /*Map<String, BackpressureMethodRateLimiter> getBackpressureMethodLimiters() {
        return backpressureMethodLimiters;
    }*/

    // Backpressure-specific rate limiter that uses qps * factor, cost limit = null
    /*protected static class BackpressureMethodRateLimiter extends MethodRateLimiter {
        // private final String methodName;
        private final int baseQps;
        private volatile double currentFactor = 1.0;
        private volatile int effectiveQps;

        BackpressureMethodRateLimiter(String methodName, int baseQps, int maxWaitRequestNum, int effectiveQps) {
            super(methodName, maxWaitRequestNum, baseQps, 0); // costLimit = 0 (null)
            // this.methodName = methodName;
            this.baseQps = baseQps;
            this.effectiveQps = effectiveQps;
            LOG.info("Created backpressure rate limiter for method: {}, baseQps: {}", methodName, baseQps);
        }

        void applyFactor(double factor) {
            this.currentFactor = factor;
            this.effectiveQps = Math.max(1, (int) (baseQps * factor));
            updateQps(getMaxWaitRequestNum(), effectiveQps);
            LOG.info("Applied factor {} to backpressure limiter for method {}, effective QPS: {}", 
                    factor, methodName, effectiveQps);
        }

        int getEffectiveQps() {
            return effectiveQps;
        }

        double getCurrentFactor() {
            return currentFactor;
        }
    }

    protected static class MethodRateLimiter {
        protected final String methodName;
        private volatile int maxWaitRequestNum;
        private Semaphore waitingSemaphore;
        private RateLimiter rateLimiter;
        private CostLimiter costLimiter;

        MethodRateLimiter(String methodName, int maxWaitRequestNum, int qps, int costLimit) {
            this.methodName = methodName;
            this.maxWaitRequestNum = maxWaitRequestNum;
            if (qps > 0) {
                this.waitingSemaphore = new Semaphore(maxWaitRequestNum);
                this.rateLimiter = RateLimiter.create(qps);
            }
            this.costLimiter = costLimit > 0 ? new CostLimiter(costLimit) : null;
            LOG.info("Create rate limiter for method: {}, maxWaitRequestNum: {}, qps: {}, cost: {}", methodName,
                    maxWaitRequestNum, qps, costLimit);
        }

        boolean acquire(int cost) throws RpcRateLimitException {
            long startAt = System.nanoTime();
            boolean acquired = acquireCostLimit(costLimiter, cost);
            try {
                acquireQpsRateLimit(waitingSemaphore, rateLimiter);
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

        private boolean acquireCostLimit(CostLimiter costLimiter, int cost) throws RpcRateLimitException {
            if (costLimiter == null || cost <= 0) {
                return false;
            }
            boolean acquired = false;
            try {
                acquired = costLimiter.acquire(cost, Config.meta_service_rpc_rate_limit_wait_timeout_ms,
                        TimeUnit.MILLISECONDS);
                if (!acquired) {
                    throw new RpcRateLimitException(
                            "Meta service rpc rate limit waiting timeout for cost limit for method: "
                                    + methodName + ", requestCost: " + cost + ", currentCost: "
                                    + costLimiter.currentCost + ", limit: " + costLimiter.limit);
                }
            } catch (InterruptedException e) {
                throw new RpcRateLimitException("Meta service rpc rate limit interrupted for cost limit for method: "
                        + methodName + ", requestCost: " + cost + ", currentCost: "
                        + costLimiter.currentCost + ", limit: " + costLimiter.limit, e);
            } finally {
                if (MetricRepo.isInit && Config.isCloudMode() && !acquired) {
                    CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED.getOrAdd(methodName).increase(1L);
                }
            }
            return acquired;
        }

        @VisibleForTesting
        void acquireQpsRateLimit(Semaphore waitingSemaphore, RateLimiter rateLimiter) throws RpcRateLimitException {
            if (rateLimiter == null || waitingSemaphore == null) {
                return;
            }
            // Try to acquire waiting semaphore first to avoid too many waiting requests
            if (!waitingSemaphore.tryAcquire()) {
                if (MetricRepo.isInit && Config.isCloudMode()) {
                    CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED.getOrAdd(methodName).increase(1L);
                }
                throw new RpcRateLimitException("Meta service rpc rate limit exceeded for method: " + methodName
                        + ", too many waiting requests (max=" + maxWaitRequestNum + ")");
            }
            // Try to acquire rate limiter permit with timeout
            try {
                long timeoutMs = Config.meta_service_rpc_rate_limit_wait_timeout_ms;
                boolean acquired = rateLimiter.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
                if (!acquired) {
                    if (MetricRepo.isInit && Config.isCloudMode()) {
                        CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED.getOrAdd(methodName).increase(1L);
                    }
                    throw new RpcRateLimitException(
                            "Meta service rpc rate limit timeout for method: " + methodName + ", rate: "
                                    + rateLimiter.getRate() + ", waited " + timeoutMs + " ms");
                }
            } catch (RpcRateLimitException e) {
                throw e;
            } catch (Exception e) {
                throw new RpcRateLimitException("Failed to acquire rate limit for method: " + methodName, e);
            } finally {
                waitingSemaphore.release();
            }
        }

        // Try to acquire without throwing - returns true if acquired, false otherwise
        *//*boolean tryAcquire() {
            if (rateLimiter == null || waitingSemaphore == null) {
                return true; // No limiting
            }
            // Try to acquire waiting semaphore
            if (!waitingSemaphore.tryAcquire()) {
                return false;
            }
            // Try to acquire rate limiter permit (non-blocking)
            try {
                boolean acquired = rateLimiter.tryAcquire(0, TimeUnit.MILLISECONDS);
                if (!acquired) {
                    waitingSemaphore.release();
                    return false;
                }
                // Successfully acquired, release semaphore permit (it will be reacquired in actual acquire)
                waitingSemaphore.release();
                return true;
            } catch (Exception e) {
                waitingSemaphore.release();
                return false;
            }
        }*//*

        void release(int cost) {
            if (costLimiter != null && cost > 0) {
                costLimiter.release(cost);
            }
        }

        void update(int maxWaitRequestNum, int qps, int costLimit) {
            updateQps(maxWaitRequestNum, qps);
            updateCostLimit(costLimit);
        }

        protected void updateQps(int maxWaitRequestNum, int qps) {
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
            LOG.info("Update rate limiter for method: {}, maxWaitRequestNum: {}, qps: {}", methodName,
                    maxWaitRequestNum, qps);
        }

        private void updateCostLimit(int costLimit) {
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

        // only used for testing
        RateLimiter getRateLimiter() {
            return rateLimiter;
        }

        // only used for testing
        int getAllowWaiting() {
            return waitingSemaphore != null ? waitingSemaphore.availablePermits() : -1;
        }

        // only used for testing
        CostLimiter getCostLimiter() {
            return costLimiter;
        }

        // Get max wait request number - for subclass access
        protected int getMaxWaitRequestNum() {
            return maxWaitRequestNum;
        }
    }

    protected static class CostLimiter {
        private volatile int limit;
        private int currentCost;
        private final Lock lock = new ReentrantLock(true);
        private final Condition condition = lock.newCondition();

        CostLimiter(int limit) {
            if (limit < 0) {
                throw new IllegalArgumentException("limit must be >= 0");
            }
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

        boolean acquire(int cost) throws InterruptedException, RpcRateLimitException {
            if (cost > limit) {
                throw new RpcRateLimitException("Cost " + cost + " exceeds the limit " + limit);
            }
            long nanos = TimeUnit.MILLISECONDS.toNanos(Config.meta_service_rpc_rate_limit_wait_timeout_ms);
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

        // only used for testing
        int getCurrentCost() {
            lock.lock();
            try {
                return currentCost;
            } finally {
                lock.unlock();
            }
        }
    }*/

    public static int getRequestCost(String methodName, Object request) {
        if (methodName.equals("getVersion")) {
            if (request == null || !(request instanceof Cloud.GetVersionRequest)) {
                LOG.warn("Failed to get request cost for method: {}, invalid request: {}", methodName, request);
                return 1;
            }
            Cloud.GetVersionRequest getVersionRequest = (Cloud.GetVersionRequest) request;
            if (getVersionRequest.hasBatchMode() && getVersionRequest.getBatchMode()) {
                int cost = getVersionRequest.getDbIdsCount();
                if (Config.meta_service_rpc_cost_clamped_to_limit_enabled) {
                    int limit = getInstance().getMethodTotalCostLimit(methodName);
                    if (limit > 0 && cost > limit) {
                        cost = limit;
                        LOG.info("Clamped cost: {} for method: {} to limit: {}", getVersionRequest.getDbIdsCount(),
                                methodName, limit);
                    }
                }
                return cost;
            } else {
                return 1;
            }
        }
        // TODO the cost of other methods is not supported now
        return 1;
    }

    public void setAdaptiveFactor(double factor) {
        // Parse phase1 methods from config and add to tracked methods
        String phase1Config = Config.meta_service_rpc_adaptive_throttle_phase1_methods;
        if (phase1Config != null && !phase1Config.isEmpty()) {
            for (String method : phase1Config.split(",")) {
                String trimmed = method.trim();
                if (!trimmed.isEmpty()) {
                    adaptiveThrottleMethods.add(trimmed);
                }
            }
        }

        for (Entry<String, BackpressureQpsRateLimiter> entry : backpressureLimiters.entrySet()) {
            BackpressureQpsRateLimiter limiter = entry.getValue();
            limiter.applyFactor(factor);
        }

        // Apply factor to all tracked methods using separate backpressure limiters
        /*int appliedCount = 0;
        for (String methodName : adaptiveThrottleMethods) {
            // Check if method should be throttled based on phase config
            boolean isPhase1 = isPhase1Method(methodName);
            boolean isPhase2 = Config.meta_service_rpc_adaptive_throttle_phase2_enabled;

            if (!isPhase1 && !isPhase2) {
                continue;
            }

            // Get or create backpressure limiter for this method (separate from active rate limiting)
            BackpressureMethodRateLimiter limiter = backpressureMethodLimiters.get(methodName);
            if (limiter == null) {
                // Create backpressure limiter with base QPS
                int baseQps = getMethodTotalQps(methodName, Config.meta_service_rpc_rate_limit_default_qps_per_core);
                int maxWaitRequestNum = Config.meta_service_rpc_rate_limit_max_waiting_request_num;

                // Only create if baseQps > 0
                if (baseQps > 0) {
                    limiter = new BackpressureMethodRateLimiter(methodName, baseQps, maxWaitRequestNum);
                    BackpressureMethodRateLimiter existing = backpressureMethodLimiters.putIfAbsent(methodName, limiter);
                    if (existing != null) {
                        limiter = existing; // Use existing if another thread created it
                    }
                }
            }

            if (limiter != null) {
                limiter.applyFactor(factor);
                appliedCount++;
            }
        }

        // Also apply to existing backpressure limiters not in adaptiveThrottleMethods
        for (BackpressureMethodRateLimiter limiter : backpressureMethodLimiters.values()) {
            if (!adaptiveThrottleMethods.contains(limiter.methodName)) {
                limiter.applyFactor(factor);
                appliedCount++;
            }
        }*/

        LOG.info("Applied adaptive factor {} to {} backpressure method limiters", factor);
    }

    private boolean isPhase1Method(String methodName) {
        String phase1Config = Config.meta_service_rpc_adaptive_throttle_phase1_methods;
        if (phase1Config == null || phase1Config.isEmpty()) {
            // Empty config means all methods are considered phase1
            return true;
        }
        String[] phase1Methods = phase1Config.split(",");
        for (String m : phase1Methods) {
            if (m.trim().equals(methodName)) {
                return true;
            }
        }
        return false;
    }

    @VisibleForTesting
    public void reset() {
        // methodLimiters.clear();
        // backpressureMethodLimiters.clear();
        methodQpsConfig.clear();
        lastQpsConfig = "";
        // adaptiveThrottleMethods.clear();
    }
}
