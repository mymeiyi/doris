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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MetaServiceRateLimiter {
    private static final Logger LOG = LogManager.getLogger(MetaServiceRateLimiter.class);

    private final Map<String, MethodRateLimiter> methodLimiters = new ConcurrentHashMap<>();
    private final Map<String, Integer> methodQpsConfig = new ConcurrentHashMap<>();
    private volatile String lastQpsConfig = "";

    private static volatile MetaServiceRateLimiter instance;

    private MetaServiceRateLimiter() {
        reloadConfig();
        if (Config.meta_service_rpc_adaptive_throttle_enabled) {
            MetaServiceAdaptiveThrottle.getInstance().setFactorChangeListener(this::setAdaptiveFactor);
        }
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

    public void reloadConfig() {
        String currentConfig = Config.meta_service_rpc_rate_limit_qps_config;
        if (!currentConfig.equals(lastQpsConfig)) {
            synchronized (this) {
                if (!currentConfig.equals(lastQpsConfig)) {
                    parseQpsConfig(currentConfig);
                    methodLimiters.clear();
                    lastQpsConfig = currentConfig;
                    LOG.info("Reloaded meta service RPC rate limit config: {}", currentConfig);
                }
            }
        }
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
                    LOG.info("Configured rate limit for method {}: {} QPS", methodName, qps);
                } catch (NumberFormatException e) {
                    LOG.warn("Invalid QPS config entry: {}", entry);
                }
            }
        }
    }

    private MethodRateLimiter getMethodLimiter(String methodName) {
        return methodLimiters.computeIfAbsent(methodName, name -> {
            int qps = methodQpsConfig.getOrDefault(name, Config.meta_service_rpc_rate_limit_default_qps);
            int maxWaiting = Config.meta_service_rpc_rate_limit_max_waiting;
            return new MethodRateLimiter(name, qps, maxWaiting);
        });
    }

    public void acquire(String methodName) throws RpcRateLimitException {
        if (!Config.meta_service_rpc_rate_limit_enabled) {
            return;
        }

        if (!Config.meta_service_rpc_rate_limit_qps_config.equals(lastQpsConfig)) {
            reloadConfig();
        }

        MethodRateLimiter limiter = getMethodLimiter(methodName);
        limiter.acquire();
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

    private static class MethodRateLimiter {
        private final String methodName;
        private final RateLimiter rateLimiter;
        private final Semaphore waitingSemaphore;
        private final int maxWaiting;
        private final int configuredQps;

        private final AtomicLong throttledCount = new AtomicLong(0);
        private final AtomicLong rejectedCount = new AtomicLong(0);

        MethodRateLimiter(String methodName, int qps, int maxWaiting) {
            this.methodName = methodName;
            this.maxWaiting = maxWaiting;
            this.configuredQps = qps;
            this.rateLimiter = qps > 0 ? RateLimiter.create(qps) : RateLimiter.create(Double.MAX_VALUE);
            this.waitingSemaphore = new Semaphore(maxWaiting);
            LOG.info("Created rate limiter for method {}: qps={}, maxWaiting={}",
                    methodName, qps, maxWaiting);
        }

        void applyAdaptiveFactor(double factor) {
            if (configuredQps <= 0) {
                return;
            }
            double effectiveQps = Math.max(1.0, configuredQps * factor);
            rateLimiter.setRate(effectiveQps);
        }

        void acquire() throws RpcRateLimitException {
            if (!waitingSemaphore.tryAcquire()) {
                rejectedCount.incrementAndGet();
                updateMetrics(true);
                throw new RpcRateLimitException(
                    "Meta service RPC rate limit exceeded for method: " + methodName
                    + ", too many waiting requests (max=" + maxWaiting + ")");
            }

            try {
                long timeoutMs = Config.meta_service_rpc_rate_limit_wait_timeout_ms;
                boolean acquired = rateLimiter.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);

                if (!acquired) {
                    waitingSemaphore.release();
                    throttledCount.incrementAndGet();
                    updateMetrics(false);
                    throw new RpcRateLimitException(
                        "Meta service RPC rate limit timeout for method: " + methodName
                        + ", waited " + timeoutMs + "ms");
                }
            } catch (RpcRateLimitException e) {
                throw e;
            } catch (Exception e) {
                waitingSemaphore.release();
                throw new RpcRateLimitException(
                    "Failed to acquire rate limit for method: " + methodName, e);
            }
        }

        void release() {
            waitingSemaphore.release();
        }

        private void updateMetrics(boolean rejected) {
            if (MetricRepo.isInit && Config.isCloudMode()) {
                if (rejected) {
                    CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_REJECTED
                            .getOrAdd(methodName).increase(1L);
                } else {
                    CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED
                            .getOrAdd(methodName).increase(1L);
                }
            }
        }
    }

    public void setAdaptiveFactor(double factor) {
        for (MethodRateLimiter limiter : methodLimiters.values()) {
            limiter.applyAdaptiveFactor(factor);
        }
        LOG.info("Applied adaptive factor {} to {} method limiters", factor, methodLimiters.size());
    }

    @VisibleForTesting
    public void reset() {
        methodLimiters.clear();
        methodQpsConfig.clear();
        lastQpsConfig = "";
    }

    @VisibleForTesting
    public static void setInstanceForTest(MetaServiceRateLimiter testInstance) {
        instance = testInstance;
    }

    @VisibleForTesting
    public static void resetInstance() {
        instance = null;
    }
}
