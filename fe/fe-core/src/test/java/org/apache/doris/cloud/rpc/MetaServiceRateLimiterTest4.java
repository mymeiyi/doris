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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MetaServiceRateLimiterTest4 {

    // Original config values for teardown
    private boolean originalRateLimitEnabled;
    private int originalDefaultQps;
    private int originalMaxWaitRequestNum;
    private String originalQpsConfig;
    private String originalCostConfig;
    private boolean originalAdaptiveThrottleEnabled;
    private String originalAdaptiveThrottleMethods;
    private boolean originalCostClampedEnabled;

    @Before
    public void setUp() {
        // Save original config values
        originalRateLimitEnabled = Config.meta_service_rpc_rate_limit_enabled;
        originalDefaultQps = Config.meta_service_rpc_rate_limit_default_qps_per_core;
        originalMaxWaitRequestNum = Config.meta_service_rpc_rate_limit_max_waiting_request_num;
        originalQpsConfig = Config.meta_service_rpc_rate_limit_qps_per_core_config;
        originalCostConfig = Config.meta_service_rpc_cost_limit_per_core_config;
        originalAdaptiveThrottleEnabled = Config.meta_service_rpc_adaptive_throttle_enabled;
        originalAdaptiveThrottleMethods = Config.meta_service_rpc_adaptive_throttle_methods;
        originalCostClampedEnabled = Config.meta_service_rpc_cost_clamped_to_limit_enabled;

        // Reset singleton for testing
        resetSingleton();
    }

    @After
    public void tearDown() {
        // Restore original config values
        Config.meta_service_rpc_rate_limit_enabled = originalRateLimitEnabled;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = originalDefaultQps;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = originalMaxWaitRequestNum;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = originalQpsConfig;
        Config.meta_service_rpc_cost_limit_per_core_config = originalCostConfig;
        Config.meta_service_rpc_adaptive_throttle_enabled = originalAdaptiveThrottleEnabled;
        Config.meta_service_rpc_adaptive_throttle_methods = originalAdaptiveThrottleMethods;
        Config.meta_service_rpc_cost_clamped_to_limit_enabled = originalCostClampedEnabled;

        // Reset singleton for testing
        resetSingleton();
    }

    private void resetSingleton() {
        try {
            Field instanceField = MetaServiceRateLimiter.class.getDeclaredField("instance");
            instanceField.setAccessible(true);
            instanceField.set(null, (MetaServiceRateLimiter) null);
        } catch (Exception e) {
            // Ignore
        }
    }

    // =========================================================================
    // Test: isConfigChanged() method
    // =========================================================================

    @Test
    public void testIsConfigChanged() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 20;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";
        Config.meta_service_rpc_cost_limit_per_core_config = "";
        Config.meta_service_rpc_adaptive_throttle_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_methods = "";
        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // change nothing
        Assert.assertFalse(limiter.isConfigChanged());

        // change meta_service_rpc_rate_limit_enabled
        Config.meta_service_rpc_rate_limit_enabled = false;
        Assert.assertTrue(limiter.isConfigChanged());

        // change meta_service_rpc_rate_limit_default_qps_per_core
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 20;
        Assertions.assertTrue(limiter.isConfigChanged());

        // change meta_service_rpc_rate_limit_max_waiting_request_num
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 30;
        Assertions.assertTrue(limiter.isConfigChanged());

        // change meta_service_rpc_rate_limit_qps_per_core_config
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "test1:10";
        Assertions.assertTrue(limiter.isConfigChanged());

        // change meta_service_rpc_cost_limit_per_core_config
        Config.meta_service_rpc_cost_limit_per_core_config = "test1:10";
        Assertions.assertTrue(limiter.isConfigChanged());

        // change meta_service_rpc_adaptive_throttle_enabled
        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Assertions.assertTrue(limiter.isConfigChanged());

        // change meta_service_rpc_adaptive_throttle_methods
        Config.meta_service_rpc_adaptive_throttle_methods = "method1,method2";
        Assertions.assertTrue(limiter.isConfigChanged());
    }

    // =========================================================================
    // Test: reloadAdaptiveThrottleConfig() method
    // =========================================================================

    @Test
    public void testReloadAdaptiveThrottleConfig() {
        Config.meta_service_rpc_rate_limit_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_methods = "method1,method2";
        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Enable adaptive throttle first
        Assert.assertFalse(limiter.reloadConfig());
        Set<String> adaptiveMethods = limiter.getAdaptiveThrottleMethods();
        Assert.assertEquals(2, adaptiveMethods.size());
        Assert.assertTrue(adaptiveMethods.contains("method1"));
        Assert.assertTrue(adaptiveMethods.contains("method2"));

        // change meta_service_rpc_adaptive_throttle_methods
        Config.meta_service_rpc_adaptive_throttle_methods = "method1, method3";
        Assert.assertTrue(limiter.reloadConfig());
        adaptiveMethods = limiter.getAdaptiveThrottleMethods();
        Assert.assertEquals(2, adaptiveMethods.size());
        Assert.assertTrue(adaptiveMethods.contains("method1"));
        Assert.assertTrue(adaptiveMethods.contains("method3"));

        // Disable adaptive throttle
        Config.meta_service_rpc_adaptive_throttle_enabled = false;
        Assert.assertTrue(limiter.reloadConfig());
        adaptiveMethods = limiter.getAdaptiveThrottleMethods();
        Assert.assertTrue(adaptiveMethods.isEmpty());
    }

    @Test
    public void testReloadAdaptiveThrottleConfig_NullMethods() {
        Config.meta_service_rpc_rate_limit_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_methods = null;
        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        Assert.assertFalse(limiter.reloadConfig());
        Set<String> adaptiveMethods = limiter.getAdaptiveThrottleMethods();
        Assert.assertTrue(adaptiveMethods.isEmpty());

        Config.meta_service_rpc_adaptive_throttle_methods = "";
        Assert.assertTrue(limiter.reloadConfig());
        adaptiveMethods = limiter.getAdaptiveThrottleMethods();
        Assert.assertTrue(adaptiveMethods.isEmpty());
    }

    // =========================================================================
    // Test: reloadRateLimiterConfig() method
    // =========================================================================

    @Test
    public void testReloadRateLimiterConfig() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:10";
        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(2);

        Assert.assertFalse(limiter.reloadConfig());
        Map<String, Integer> qpsConfig = limiter.getMethodQpsConfig();
        Assert.assertEquals(1, qpsConfig.size());
        Assert.assertEquals(10, qpsConfig.get("method1").intValue());

        Config.meta_service_rpc_cost_limit_per_core_config = "method1:30; method2:20";
        Assert.assertFalse(limiter.reloadConfig());
        qpsConfig = limiter.getMethodQpsConfig();
        Assert.assertEquals(1, qpsConfig.size());
        Assert.assertEquals(10, qpsConfig.get("method1").intValue());
        Map<String, Integer> costConfig = limiter.getMethodCostConfig();
        Assert.assertEquals(2, costConfig.size());
        Assert.assertEquals(30, costConfig.get("method1").intValue());
        Assert.assertEquals(20, costConfig.get("method2").intValue());

        Config.meta_service_rpc_cost_limit_per_core_config = "invalidformat;another:bad;negative:-10;normal:100";
        Assert.assertFalse(limiter.reloadConfig());
        qpsConfig = limiter.getMethodQpsConfig();
        Assert.assertEquals(1, qpsConfig.size());
        Assert.assertEquals(100, qpsConfig.get("normal").intValue());

        // Disable rate limiter
        Config.meta_service_rpc_rate_limit_enabled = false;
        limiter.reloadConfig();
        qpsConfig = limiter.getMethodQpsConfig();
        Assert.assertEquals(0, qpsConfig.size());
        costConfig = limiter.getMethodCostConfig();
        Assert.assertEquals(0, costConfig.size());
    }

    // =========================================================================
    // Test: acquire() release() method
    // =========================================================================

    @Test
    public void testAcquire_BothDisabled() {
        Config.meta_service_rpc_rate_limit_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_enabled = false;
        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("anyMethod", 1)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(0, limiter.getQpsLimiters().size());
        Assert.assertEquals(0, limiter.getCostLimiters().size());
        Assert.assertEquals(0, limiter.getBackpressureQpsLimiters().size());
    }

    @Test
    public void testAcquire_QpsLimitEnabled() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_enabled = false;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;
        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // qps enabled, cost disabled
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 1)));
        // Returns false because method1 is not in Config.meta_service_rpc_cost_limit_per_core_config
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(1, limiter.getQpsLimiters().size());
        Assert.assertEquals(0, limiter.getCostLimiters().size());
        Assert.assertEquals(0, limiter.getBackpressureQpsLimiters().size());

        // qps enabled, cost enabled
        Config.meta_service_rpc_cost_limit_per_core_config = "method1:10";
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 1)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(1, limiter.getQpsLimiters().size());
        Assert.assertEquals(1, limiter.getCostLimiters().size());
        Assert.assertEquals(1, limiter.getCostLimiters().get("method1").getCurrentCost());
        limiter.release("method1", 1);
        Assert.assertEquals(0, limiter.getCostLimiters().get("method1").getCurrentCost());

        // qps enabled, cost enabled but exceeds limit
        Assertions.assertThrows(RpcRateLimitException.class,
                () -> limiter.acquire("method1", 11));
        Assert.assertEquals(0, limiter.getCostLimiters().get("method1").getCurrentCost());

        // qps disabled, cost disabled
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:0";
        Config.meta_service_rpc_cost_limit_per_core_config = "method1:0";
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 1)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(0, limiter.getQpsLimiters().size());
        Assert.assertEquals(0, limiter.getCostLimiters().size());

        // qps disabled, cost enabled
        Config.meta_service_rpc_cost_limit_per_core_config = "method1:10";
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 5)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(0, limiter.getQpsLimiters().size());
        Assert.assertEquals(1, limiter.getCostLimiters().size());
        Assert.assertEquals(5, limiter.getCostLimiters().get("method1").getCurrentCost());
        limiter.release("method1", 5);
    }

    @Test
    public void testAcquire_AdaptiveThrottleEnabled() {
        Config.meta_service_rpc_rate_limit_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;
        Config.meta_service_rpc_adaptive_throttle_methods = "method1";
        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);
        try (MockedStatic<MetaServiceAdaptiveThrottle> mockedStatic = Mockito.mockStatic(
                MetaServiceAdaptiveThrottle.class)) {
            MetaServiceAdaptiveThrottle throttle = Mockito.mock(MetaServiceAdaptiveThrottle.class);
            mockedStatic.when(MetaServiceAdaptiveThrottle::getInstance).thenReturn(throttle);
            Mockito.when(throttle.getFactor()).thenReturn(0.9);

            AtomicBoolean acquired = new AtomicBoolean(false);
            Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 1)));
            Assert.assertFalse(acquired.get());
            Assert.assertEquals(0, limiter.getQpsLimiters().size());
            Assert.assertEquals(0, limiter.getCostLimiters().size());
            Assert.assertEquals(1, limiter.getBackpressureQpsLimiters().size());
            Assert.assertEquals(100, limiter.getBackpressureQpsLimiters().get("method1").getBaseQps());
            Assert.assertEquals(90, limiter.getBackpressureQpsLimiters().get("method1").getRateLimiter().getRate(),
                    0.01);
        }
    }

    @Test
    public void testAcquire_BothEnabled() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;
        Config.meta_service_rpc_adaptive_throttle_methods = "method1";
        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);
        try (MockedStatic<MetaServiceAdaptiveThrottle> mockedStatic = Mockito.mockStatic(
                MetaServiceAdaptiveThrottle.class)) {
            MetaServiceAdaptiveThrottle throttle = Mockito.mock(MetaServiceAdaptiveThrottle.class);
            mockedStatic.when(MetaServiceAdaptiveThrottle::getInstance).thenReturn(throttle);
            Mockito.when(throttle.getFactor()).thenReturn(0.9);

            // qps enabled, cost disabled
            AtomicBoolean acquired = new AtomicBoolean(false);
            Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 1)));
            // Returns false because method1 is not in Config.meta_service_rpc_cost_limit_per_core_config
            Assert.assertFalse(acquired.get());
            Assert.assertEquals(1, limiter.getQpsLimiters().size());
            Assert.assertEquals(0, limiter.getCostLimiters().size());
            Assert.assertEquals(1, limiter.getBackpressureQpsLimiters().size());
            Assert.assertEquals(100, limiter.getBackpressureQpsLimiters().get("method1").getBaseQps());
            Assert.assertEquals(90, limiter.getBackpressureQpsLimiters().get("method1").getRateLimiter().getRate(),
                    0.01);

            // qps enabled, cost enabled
            Config.meta_service_rpc_cost_limit_per_core_config = "method1:10";
            Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 1)));
            Assert.assertTrue(acquired.get());
            Assert.assertEquals(1, limiter.getQpsLimiters().size());
            Assert.assertEquals(1, limiter.getCostLimiters().size());
            Assert.assertEquals(1, limiter.getCostLimiters().get("method1").getCurrentCost());
            limiter.release("method1", 1);
            Assert.assertEquals(0, limiter.getCostLimiters().get("method1").getCurrentCost());

            // qps enabled, cost enabled but exceeds limit
            Assertions.assertThrows(RpcRateLimitException.class,
                    () -> limiter.acquire("method1", 11));
            Assert.assertEquals(0, limiter.getCostLimiters().get("method1").getCurrentCost());

            // qps disabled, cost disabled
            Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:0";
            Config.meta_service_rpc_cost_limit_per_core_config = "method1:0";
            Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 1)));
            Assert.assertFalse(acquired.get());
            Assert.assertEquals(0, limiter.getQpsLimiters().size());
            Assert.assertEquals(0, limiter.getCostLimiters().size());

            // qps disabled, cost enabled
            Config.meta_service_rpc_cost_limit_per_core_config = "method1:10";
            Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 5)));
            Assert.assertTrue(acquired.get());
            Assert.assertEquals(0, limiter.getQpsLimiters().size());
            Assert.assertEquals(1, limiter.getCostLimiters().size());
            limiter.release("method1", 5);
            Assert.assertEquals(1, limiter.getBackpressureQpsLimiters().size());
        }
    }

    @Test
    public void testIntegration_ConcurrentAcquire() throws InterruptedException {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 50; // High QPS
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 10;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_cost_limit_per_core_config = "concurrentBoth:5";
        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int cost = 1;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    boolean acquired = limiter.acquire("concurrentBoth", cost);
                    if (acquired) {
                        successCount.incrementAndGet();
                    } else {
                        failCount.incrementAndGet();
                    }
                } catch (RpcRateLimitException e) {
                    failCount.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startLatch.countDown();
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        // With cost limit 5 and each request costing 1, max 5 should succeed
        // Others should fail due to cost limit
        Assert.assertTrue("Expected some successes", successCount.get() > 0);
        Assert.assertTrue("Expected some failures", failCount.get() > 0);
        Assert.assertEquals(5, successCount.get());
        Assert.assertEquals(5, failCount.get());

        // Release all successful acquisitions
        for (int i = 0; i < successCount.get(); i++) {
            limiter.release("concurrentBoth", 1);
        }
    }
}
