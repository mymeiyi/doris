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

import org.apache.doris.cloud.rpc.RpcRateLimiter.QpsLimiter;
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
    private long originalWaitTimeoutMs;
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
        originalWaitTimeoutMs = Config.meta_service_rpc_rate_limit_wait_timeout_ms;
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
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = originalWaitTimeoutMs;
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
        Assert.assertTrue(limiter.reloadConfig());
        qpsConfig = limiter.getMethodQpsConfig();
        Assert.assertEquals(1, qpsConfig.size());
        Assert.assertEquals(10, qpsConfig.get("method1").intValue());
        Map<String, Integer> costConfig = limiter.getMethodCostConfig();
        Assert.assertEquals(2, costConfig.size());
        Assert.assertEquals(30, costConfig.get("method1").intValue());
        Assert.assertEquals(20, costConfig.get("method2").intValue());

        Config.meta_service_rpc_rate_limit_qps_per_core_config = "invalidformat;another:bad;negative:-10;normal:100";
        Assert.assertTrue(limiter.reloadConfig());
        qpsConfig = limiter.getMethodQpsConfig();
        Assert.assertEquals(2, qpsConfig.size());
        Assert.assertEquals(100, qpsConfig.get("normal").intValue());
        Assert.assertEquals(-10, qpsConfig.get("negative").intValue());

        // Disable rate limiter
        Config.meta_service_rpc_rate_limit_enabled = false;
        Assert.assertTrue(limiter.reloadConfig());
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
    public void testConcurrentAcquire() throws InterruptedException {
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

    @Test
    public void testCostClampedToLimit() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 10;
        Config.meta_service_rpc_cost_limit_per_core_config = "getVersion:10";
        Config.meta_service_rpc_cost_clamped_to_limit_enabled = true;

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(2);
        Assert.assertEquals(10, limiter.getClampedCost("getVersion", 10));
        Assert.assertEquals(20, limiter.getClampedCost("getVersion", 30));
        Assert.assertEquals(40, limiter.getClampedCost("other", 40));
    }

    @Test
    public void testWaitTimeout() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 1;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 5;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";
        int maxWaitingFailCount = 0;
        int acquireFailCount = 0;

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);
        for (int i = 0; i < 10; i++) {
            try {
                limiter.acquire("testWaitTimeout", 0);
            } catch (RpcRateLimitException e) {
                // LOG.warn("i={}", i, e);
                if (e.getMessage().contains("too many waiting requests")) {
                    maxWaitingFailCount++;
                } else if (e.getMessage().contains("timeout for method")) {
                    acquireFailCount++;
                }
            }
        }
        Assert.assertEquals(0, maxWaitingFailCount);
        Assert.assertEquals(9, acquireFailCount);
        Assert.assertEquals(1, limiter.getQpsLimiters().size());
        QpsLimiter qpsLimiter = limiter.getQpsLimiters().get("testWaitTimeout");
        Assert.assertNotNull(qpsLimiter);
        Assert.assertEquals(5, qpsLimiter.getAllowWaiting());
    }

    @Test
    public void testConcurrentAccess() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 5000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";
        Config.meta_service_rpc_cost_limit_per_core_config = "";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    boolean acquired = limiter.acquire("testMethod", 0);
                    Assert.assertFalse(acquired); // cost limit is disabled
                    successCount.incrementAndGet();
                } catch (RpcRateLimitException e) {
                    failCount.incrementAndGet();
                } catch (InterruptedException e) {
                    failCount.incrementAndGet();
                    Thread.currentThread().interrupt();
                }
            });
        }

        startLatch.countDown();
        executor.shutdown();
        try {
            boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
            Assert.assertTrue("Executor did not terminate in the expected time", terminated);
        } catch (Exception e) {
            Assert.fail("Test was interrupted: " + e.getMessage());
        }
        int successes = successCount.get();
        int failures = failCount.get();
        Assert.assertEquals("Total results should match thread count", threadCount, successes + failures);
        Assert.assertEquals(10, successes);
    }

    @Test
    public void testAcquireWithConfigChange() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 99;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Initial acquire creates limiter with default QPS (10)
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(1, limiter.getQpsLimiters().size());
        QpsLimiter method1 = limiter.getQpsLimiters().get("method1");
        Assert.assertNotNull(method1);
        Assert.assertEquals(99, method1.getAllowWaiting());

        // Change config - this should trigger reload on next acquire
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:20";
        Assert.assertTrue(limiter.reloadConfig());
        Assert.assertEquals(1, limiter.getQpsLimiters().size());
        method1 = limiter.getQpsLimiters().get("method1");
        Assert.assertNotNull(method1);
        Assert.assertEquals(20, (int) method1.getRateLimiter().getRate());
        Assert.assertEquals(99, method1.getAllowWaiting());
    }

    @Test
    public void testReleaseAfterDisable() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Acquire to create a limiter
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(1, limiter.getQpsLimiters().size());

        // Disable rate limiter
        Config.meta_service_rpc_rate_limit_enabled = false;
        limiter.reloadConfig();

        // Release after disable should be a no-op
        // Should not throw exception
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());

        // Limiter map should be cleared after reloadConfig
        Assert.assertEquals(0, limiter.getQpsLimiters().size());
    }

    @Test
    public void testReleaseWithExistingLimiterAfterDisable() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Acquire to create a limiter
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(1, limiter.getQpsLimiters().size());

        // Directly release after disable without calling reloadConfig
        Config.meta_service_rpc_rate_limit_enabled = false;

        // Release should check enabled first and skip
        // This should not throw because release() checks enabled before accessing limiter
        Assertions.assertDoesNotThrow(() -> limiter.release("method1", 0));

        // Limiter still exists but release was skipped
        Assert.assertEquals(1, limiter.getQpsLimiters().size());

        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(0, limiter.getQpsLimiters().size());
    }

    @Test
    public void testCostLimitAcquisitionAndFailure() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_cost_limit_per_core_config = "costMethod:5";
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Acquire cost 3 - should succeed
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("costMethod", 3)));
        Assert.assertTrue(acquired.get());
        RpcRateLimiter.CostLimiter costLimiter = limiter.getCostLimiters().get("costMethod");
        Assert.assertNotNull(costLimiter);
        Assert.assertEquals(3, costLimiter.getCurrentCost());

        // Release cost 3
        limiter.release("costMethod", 3);
        Assert.assertEquals(0, costLimiter.getCurrentCost());

        // Acquire cost 4 - should succeed
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("costMethod", 4)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(4, costLimiter.getCurrentCost());

        // Release cost 4
        limiter.release("costMethod", 4);
        Assert.assertEquals(0, costLimiter.getCurrentCost());

        // Acquire cost 2 - should succeed
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("costMethod", 2)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(2, costLimiter.getCurrentCost());

        // Acquire cost 4 - should fail (2+4 > 5)
        RpcRateLimitException exception = Assertions.assertThrows(RpcRateLimitException.class,
                () -> limiter.acquire("costMethod", 4));
        Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("limit timeout"));
        Assert.assertEquals(2, costLimiter.getCurrentCost());

        // Release cost 2
        limiter.release("costMethod", 2);
        Assert.assertEquals(0, costLimiter.getCurrentCost());
    }

    @Test
    public void testCostLimitReloadUpdatesLimit() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_cost_limit_per_core_config = "reloadMethod:5";
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Acquire cost 1 - should succeed
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("reloadMethod", 1)));
        Assert.assertTrue(acquired.get());
        RpcRateLimiter.CostLimiter costLimiter = limiter.getCostLimiters().get("reloadMethod");
        Assert.assertEquals(1, costLimiter.getCurrentCost());

        // Reload config with higher limit
        Config.meta_service_rpc_cost_limit_per_core_config = "reloadMethod:8";
        limiter.reloadConfig();

        // Now acquire cost 7 - should succeed (new limit is 8)
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("reloadMethod", 7)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(8, costLimiter.getCurrentCost());

        // Release all
        limiter.release("reloadMethod", 7);
        limiter.release("reloadMethod", 1);
        Assert.assertEquals(0, costLimiter.getCurrentCost());
    }

    @Test
    public void testCostLimitWithDisableEnabledToggle() {
        // Step 1: Enable rate limit, set cost limit to 5
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_cost_limit_per_core_config = "testCostMethod:5";
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Step 2: Acquire cost 4 (should succeed, limit is 5)
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("testCostMethod", 4)));
        Assert.assertTrue(acquired.get());
        // Verify current cost is 4
        RpcRateLimiter.CostLimiter costLimiter = limiter.getCostLimiters().get("testCostMethod");
        Assert.assertNotNull(costLimiter);
        Assert.assertEquals(4, costLimiter.getCurrentCost());

        // Acquire cost 1 (should succeed, limit is 5)
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("testCostMethod", 1)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(5, costLimiter.getCurrentCost());

        // Release cost 1
        limiter.release("testCostMethod", 1);
        Assert.assertEquals(4, costLimiter.getCurrentCost());

        // Step 3: Disable rate limit, try to acquire - should return false
        Config.meta_service_rpc_rate_limit_enabled = false;
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("testCostMethod", 1)));
        Assert.assertFalse(acquired.get());
        // Current cost should still be 4 (acquire returned false when disabled)
        Assert.assertEquals(4, costLimiter.getCurrentCost());

        // Release cost 4 (does not work because limiter is null)
        limiter.release("testCostMethod", 4);
        Assert.assertEquals(4, costLimiter.getCurrentCost());
        Assert.assertNull(limiter.getCostLimiters().get("testCostMethod"));
    }

    @Test
    public void testBothRateAndCostLimiterBasic() {
        // Set both QPS and cost limits
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_cost_limit_per_core_config = "bothMethod:5";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Acquire with cost within both limits
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("bothMethod", 3)));
        Assert.assertTrue(acquired.get());

        // Verify cost limiter is created and has correct values
        QpsLimiter methodLimiter = limiter.getQpsLimiters().get("bothMethod");
        Assert.assertNotNull(methodLimiter);
        Assert.assertNotNull(methodLimiter.getRateLimiter());
        Assert.assertEquals(10, (int) methodLimiter.getRateLimiter().getRate());

        RpcRateLimiter.CostLimiter costLimiter = limiter.getCostLimiters().get("bothMethod");
        Assert.assertNotNull(costLimiter);
        Assert.assertEquals(3, costLimiter.getCurrentCost());

        // Release and verify
        limiter.release("bothMethod", 3);
        Assert.assertEquals(0, costLimiter.getCurrentCost());
    }

    @Test
    public void testCostBlocksBeforeQpsWhenBothSet() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_cost_limit_per_core_config = "costFirst:3";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Acquire cost 2 - should succeed
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("costFirst", 2)));
        Assert.assertTrue(acquired.get());
        RpcRateLimiter.CostLimiter costLimiter = limiter.getCostLimiters().get("costFirst");
        Assert.assertEquals(2, costLimiter.getCurrentCost());

        // Acquire cost 2 again - total cost would be 4, limit is 3, should fail
        RpcRateLimitException exception = Assertions.assertThrows(RpcRateLimitException.class,
                () -> limiter.acquire("costFirst", 2));
        Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("limit timeout"));

        // Verify QPS rate limiter was never touched (cost blocked first)
        QpsLimiter methodLimiter = limiter.getQpsLimiters().get("costFirst");
        Assert.assertEquals(100, methodLimiter.getAllowWaiting());

        // Release
        limiter.release("costFirst", 2);
        Assert.assertEquals(0, costLimiter.getCurrentCost());
    }

    @Test
    public void testQpsBlocksBeforeCostWhenBothSet() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 1;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 1;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 10; // Very short timeout
        Config.meta_service_rpc_cost_limit_per_core_config = "qpsFirst:100"; // High cost limit

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // First acquire - should succeed
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("qpsFirst", 10)));
        Assert.assertTrue(acquired.get());

        // Second acquire - QPS limit should block (only 1 request allowed at a time)
        RpcRateLimitException exception = Assertions.assertThrows(RpcRateLimitException.class,
                () -> limiter.acquire("qpsFirst", 10));
        Assert.assertTrue(exception.getMessage().contains("timeout")
                || exception.getMessage().contains("too many waiting requests"));

        // Release
        limiter.release("qpsFirst", 10);
    }

    @Test
    public void testReleaseOnlyAffectsCostNotQps() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_cost_limit_per_core_config = "releaseTest:5";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Acquire - uses both QPS slot and cost
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("releaseTest", 3)));
        Assert.assertTrue(acquired.get());
        RpcRateLimiter.CostLimiter costLimiter = limiter.getCostLimiters().get("releaseTest");
        Assert.assertEquals(3, costLimiter.getCurrentCost());

        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("releaseTest", 2)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(5, costLimiter.getCurrentCost());

        // Release
        limiter.release("releaseTest", 2);
        Assert.assertEquals(3, costLimiter.getCurrentCost());
    }

    @Test
    public void testReloadUpdatesBothRateAndCost() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 50;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "reloadBoth:10";
        Config.meta_service_rpc_cost_limit_per_core_config = "reloadBoth:5";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Initial acquire
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("reloadBoth", 3)));
        Assert.assertTrue(acquired.get());

        QpsLimiter methodLimiter = limiter.getQpsLimiters().get("reloadBoth");
        Assert.assertEquals(10, (int) methodLimiter.getRateLimiter().getRate());
        RpcRateLimiter.CostLimiter costLimiter = limiter.getCostLimiters().get("reloadBoth");
        Assert.assertEquals(3, costLimiter.getCurrentCost());

        // Reload with new values
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "reloadBoth:20";
        Config.meta_service_rpc_cost_limit_per_core_config = "reloadBoth:8";
        limiter.reloadConfig();

        // Verify both were updated
        methodLimiter = limiter.getQpsLimiters().get("reloadBoth");
        Assert.assertEquals(20, (int) methodLimiter.getRateLimiter().getRate());

        // Cost limit increased, can acquire more
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("reloadBoth", 5)));
        Assert.assertTrue(acquired.get());
        costLimiter = limiter.getCostLimiters().get("reloadBoth");
        Assert.assertEquals(8, costLimiter.getCurrentCost());

        // Release
        limiter.release("reloadBoth", 5);
        limiter.release("reloadBoth", 3);
    }

    @Test
    public void testCostAndQpsWithZeroDefaultQps() {
        // QPS default is 0 but method-specific cost is set
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_cost_limit_per_core_config = "zeroQps:5";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Should still work with only cost limit (QPS is 0 so no rate limiting)
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("zeroQps", 3)));
        Assert.assertTrue(acquired.get());

        QpsLimiter methodLimiter = limiter.getQpsLimiters().get("zeroQps");
        // Rate limiter should be null since QPS is 0
        Assert.assertNull(methodLimiter);
        // But cost limiter should exist
        RpcRateLimiter.CostLimiter costLimiter = limiter.getCostLimiters().get("zeroQps");
        Assert.assertNotNull(costLimiter);
        Assert.assertEquals(3, costLimiter.getCurrentCost());

        // Release
        limiter.release("zeroQps", 3);
        Assert.assertEquals(0, costLimiter.getCurrentCost());
    }

    @Test
    public void testCostAndQpsWithZeroMethodCost() {
        // Method-specific cost is 0 (disabled), but default QPS is set
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_cost_limit_per_core_config = "zeroCost:0";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Should work with only QPS limit (cost is 0)
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("zeroCost", 0)));
        Assert.assertFalse(acquired.get());

        QpsLimiter methodLimiter = limiter.getQpsLimiters().get("zeroCost");
        Assert.assertNotNull(methodLimiter.getRateLimiter());
        // Cost limiter should be null since cost config is 0
        RpcRateLimiter.CostLimiter costLimiter = limiter.getCostLimiters().get("zeroQps");
        Assert.assertNull(costLimiter);

        // Can acquire with any cost value since cost limiting is disabled
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("zeroCost", 100)));
        Assert.assertFalse(acquired.get());
    }

    @Test
    public void testConcurrentWithBothRateAndCostLimits() throws InterruptedException {
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

    @Test
    public void testCostExceedLimit() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 10;
        Config.meta_service_rpc_cost_limit_per_core_config = "exceedCost:5";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Try to acquire cost greater than limit - should not require limit
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertThrows(RpcRateLimitException.class, () -> limiter.acquire("exceedCost", 10));
        Assert.assertFalse(acquired.get());

        // Verify cost limiter was created but current cost is 0 (10 > 5)
        QpsLimiter methodLimiter = limiter.getQpsLimiters().get("exceedCost");
        Assert.assertNotNull(methodLimiter);
        RpcRateLimiter.CostLimiter costLimiter = limiter.getCostLimiters().get("exceedCost");
        Assert.assertNotNull(costLimiter);
        Assert.assertEquals(0, costLimiter.getCurrentCost());

        // Release - should only release if acquired
        if (acquired.get()) {
            limiter.release("exceedCost", 10);
        }
        Assert.assertEquals(0, costLimiter.getCurrentCost());
    }

    @Test
    public void testReleaseNonExistentMethodAndNegative() {
        // Test that releasing a non-existent method does not throw
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "negCost:5";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Release should not throw even if method doesn't exist
        Assertions.assertDoesNotThrow(() -> limiter.release("nonExistentMethod", 1));

        // Ignore cost is negative
        Assertions.assertDoesNotThrow(() -> limiter.acquire("negCost", -1));
        Assertions.assertDoesNotThrow(() -> limiter.release("negCost", -1));
    }

    @Test
    public void testZeroMaxWaiting() {
        // Test that maxWaitRequestNum=0 rejects all requests immediately
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 0;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Should fail immediately due to "too many waiting requests"
        Assertions.assertThrows(IllegalArgumentException.class, () -> limiter.acquire("zeroWait", 0));
    }

    @Test
    public void testBothQpsAndCostZero() {
        // Test that both QPS=0 and cost=0 means no rate limiting
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";
        Config.meta_service_rpc_cost_limit_per_core_config = "";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Should return false since both QPS and cost limiters are null
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("bothZero", 1)));
        Assert.assertFalse(acquired.get());

        // No method limiters should be created
        Assert.assertEquals(0, limiter.getQpsLimiters().size());
    }

    @Test
    public void testCostLimitExactBoundary() {
        // Test exact boundary: current=4, limit=5, acquire=1 should succeed, acquire=2 should fail
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_cost_limit_per_core_config = "boundary:5";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Acquire cost 4 - should succeed
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("boundary", 4)));
        Assert.assertTrue(acquired.get());

        RpcRateLimiter.CostLimiter costLimiter = limiter.getCostLimiters().get("boundary");
        Assert.assertEquals(4, costLimiter.getCurrentCost());

        // Acquire cost 1 - should succeed (4+1=5, exactly at limit)
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("boundary", 1)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(5, costLimiter.getCurrentCost());

        // Acquire cost 1 again - should fail (5+1 > 5)
        RpcRateLimitException exception = Assertions.assertThrows(RpcRateLimitException.class,
                () -> limiter.acquire("boundary", 1));
        Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("limit timeout"));
        Assert.assertEquals(5, costLimiter.getCurrentCost());

        // Release and verify
        limiter.release("boundary", 4);
        limiter.release("boundary", 1);
        Assert.assertEquals(0, costLimiter.getCurrentCost());
    }

    @Test
    public void testMultipleAcquireReleaseAccuracy() {
        // Test multiple acquire/release cycles maintain correct currentCost
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_cost_limit_per_core_config = "accuracy:10";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // Acquire 3 times
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("accuracy", 2)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(2, limiter.getCostLimiters().get("accuracy").getCurrentCost());

        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("accuracy", 3)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(5, limiter.getCostLimiters().get("accuracy").getCurrentCost());

        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("accuracy", 5)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(10, limiter.getCostLimiters().get("accuracy").getCurrentCost());

        // Release one
        limiter.release("accuracy", 3);
        Assert.assertEquals(7, limiter.getCostLimiters().get("accuracy").getCurrentCost());

        // Release all
        limiter.release("accuracy", 2);
        limiter.release("accuracy", 5);
        Assert.assertEquals(0, limiter.getCostLimiters().get("accuracy").getCurrentCost());
    }

    @Test
    public void testReloadRecreatesRemovedMethod() {
        // Test that re-adding a removed method works correctly
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_cost_limit_per_core_config = "recreate:5";

        MetaServiceRateLimiter limiter = new MetaServiceRateLimiter(1);

        // First acquire
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("recreate", 1)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(1, limiter.getCostLimiters().get("recreate").getCurrentCost());

        // Remove from config
        Config.meta_service_rpc_cost_limit_per_core_config = "";
        limiter.reloadConfig();

        // Method should be removed
        Assert.assertNull(limiter.getQpsLimiters().get("recreate"));

        // Add back to config
        Config.meta_service_rpc_cost_limit_per_core_config = "recreate:10";
        limiter.reloadConfig();

        // Should work again
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("recreate", 5)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(5, limiter.getCostLimiters().get("recreate").getCurrentCost());

        limiter.release("recreate", 5);
        limiter.release("recreate", 1);
    }

    /*@Test
    public void testCostLimitPassButRateLimitFail() {
        class MockMethodRateLimiter extends MetaServiceRateLimiter.QpsLimiter {
            MockMethodRateLimiter(String methodName, int maxWaitRequestNum, int qps, int costLimit) {
                super(methodName, maxWaitRequestNum, qps, costLimit);
            }

            @Override
            void acquireQpsRateLimit(Semaphore waitingSemaphore, RateLimiter rateLimiter) throws RpcRateLimitException {
                throw new RpcRateLimitException("QPS limit exceeded");
            }
        }

        QpsLimiter methodRateLimiter = new MockMethodRateLimiter("testMethod", 8, 1, 7);
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertThrows(RpcRateLimitException.class, () -> acquired.set(methodRateLimiter.acquire(3)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(0, methodRateLimiter.getCostLimiter().getCurrentCost());
    }*/
}
