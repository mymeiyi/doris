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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class MetaServiceRateLimiter {
    private static final Logger LOG = LogManager.getLogger(MetaServiceRateLimiter.class);

    private static volatile MetaServiceRateLimiter instance;
    private volatile boolean lastEnabled = false;
    private volatile int lastDefaultQps = 0;
    private volatile String lastQpsConfig = "";

    private final Map<String, Integer> methodQpsConfig = new ConcurrentHashMap<>();
    private final Map<String, MethodRateLimiter> methodLimiters = new ConcurrentHashMap<>();

    public MetaServiceRateLimiter() {
        reloadConfig();
    }

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

    private boolean isConfigChanged() {
        return Config.meta_service_rpc_rate_limit_enabled != lastEnabled
                || Config.meta_service_rpc_rate_limit_default_qps_per_core != lastDefaultQps
                || !Objects.equals(Config.meta_service_rpc_rate_limit_qps_per_core_config, lastQpsConfig);
    }

    public boolean reloadConfig() {
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
                lastEnabled = enabled;
                return true;
            }
            // Parse the QPS config
            int defaultQpsPerCore = Config.meta_service_rpc_rate_limit_default_qps_per_core;
            String currentConfig = Config.meta_service_rpc_rate_limit_qps_per_core_config;
            parseQpsConfig(currentConfig);
            // Update existing limiters
            List<String> toRemove = new ArrayList<>();
            for (Entry<String, MethodRateLimiter> entry : methodLimiters.entrySet()) {
                String methodName = entry.getKey();
                int qps = getMethodTotalQps(methodName, defaultQpsPerCore);
                if (qps <= 0) {
                    toRemove.add(methodName);
                    continue;
                }
                MethodRateLimiter limiter = entry.getValue();
                limiter.updateQps(qps);
            }
            LOG.info("Removed zero QPS rate limiter for methods: {}", toRemove);
            for (String methodName : toRemove) {
                methodLimiters.remove(methodName);
            }
            // Update last config
            lastEnabled = enabled;
            lastDefaultQps = defaultQpsPerCore;
            lastQpsConfig = currentConfig;
            LOG.info("Reloaded meta service RPC rate limit enabled: {}, defaultQps: {}, config: {}",
                    lastEnabled, lastDefaultQps, lastQpsConfig);
        }
        return true;
    }

    private void parseQpsConfig(String config) {
        methodQpsConfig.clear();
        if (config == null || config.isEmpty()) {
            return;
        }

        String[] entries = config.split(";");
        for (String entry : entries) {
            String[] parts = entry.trim().split(":");
            if (parts.length == 2) {
                try {
                    String methodName = parts[0].trim();
                    int qps = Integer.parseInt(parts[1].trim());
                    methodQpsConfig.put(methodName, qps);
                    LOG.debug("Configured meta service RPC rate limit for method {}: {} QPS", methodName, qps);
                } catch (NumberFormatException e) {
                    LOG.warn("Invalid QPS config entry: {}", entry);
                }
            } else {
                LOG.warn("Invalid QPS config entry: {}", entry);
            }
        }
    }

    private int getMethodTotalQps(String methodName, int defaultQpsPerCore) {
        int qpsPerCore = methodQpsConfig.getOrDefault(methodName, defaultQpsPerCore);
        if (qpsPerCore <= 0) {
            return 0;
        }
        return qpsPerCore * getAvailableProcessors();
    }

    protected int getAvailableProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }

    private MethodRateLimiter getMethodLimiter(String methodName) {
        return methodLimiters.compute(methodName, (name, limiter) -> {
            if (limiter != null) {
                return limiter;
            }
            int processors = getAvailableProcessors();
            int qps = methodQpsConfig.getOrDefault(name, Config.meta_service_rpc_rate_limit_default_qps_per_core)
                    * processors;
            if (qps > 0) {
                return new MethodRateLimiter(name, qps, Config.meta_service_rpc_rate_limit_max_waiting);
            }
            return null;
        });
    }

    public void acquire(String methodName) throws RpcRateLimitException {
        if (isConfigChanged()) {
            reloadConfig();
        }

        if (!Config.meta_service_rpc_rate_limit_enabled) {
            return;
        }

        MethodRateLimiter limiter = getMethodLimiter(methodName);
        if (limiter != null) {
            limiter.acquire();
        }
    }

    public void release(String methodName) {
        if (!Config.meta_service_rpc_rate_limit_enabled) {
            return;
        }

        MethodRateLimiter limiter = methodLimiters.get(methodName);
        if (limiter != null) {
            limiter.release();
        }
    }

    protected class MethodRateLimiter {
        private final String methodName;
        private volatile int maxWaiting;
        private final RateLimiter rateLimiter;
        private final Semaphore waitingSemaphore;

        MethodRateLimiter(String methodName, int qps, int maxWaiting) {
            this.methodName = methodName;
            this.maxWaiting = maxWaiting;
            this.rateLimiter = qps > 0 ? RateLimiter.create(qps) : RateLimiter.create(Double.MAX_VALUE);
            this.waitingSemaphore = new Semaphore(maxWaiting);
            LOG.info("Create rate limiter for method={}, qps={}, maxWaiting={}", methodName, qps, maxWaiting);
        }

        void acquire() throws RpcRateLimitException {
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
                    release();
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
                release();
                throw new RpcRateLimitException(
                    "Failed to acquire rate limit for method: " + methodName, e);
            } finally {
                if (MetricRepo.isInit && Config.isCloudMode()) {
                    CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED_LATENCY.getOrAdd(methodName)
                            .update(System.currentTimeMillis() - startTime);
                }
            }
        }

        void release() {
            waitingSemaphore.release();
        }

        void updateQps(int qps) {
            rateLimiter.setRate(qps);
            LOG.info("Updated rate limiter for method {}: qps={}", methodName, qps);
        }

        @VisibleForTesting
        RateLimiter getRateLimiter() {
            return rateLimiter;
        }

        @VisibleForTesting
        public int getAllowWaiting() {
            return waitingSemaphore.availablePermits();
        }
    }

    @VisibleForTesting
    public Map<String, Integer> getMethodQpsConfig() {
        return methodQpsConfig;
    }

    @VisibleForTesting
    public Map<String, MethodRateLimiter> getMethodLimiters() {
        return methodLimiters;
    }
}
