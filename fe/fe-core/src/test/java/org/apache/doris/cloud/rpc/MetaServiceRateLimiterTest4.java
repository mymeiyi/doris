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

/**
 * Comprehensive unit tests for MetaServiceRateLimiter.
 * Tests cover: configuration parsing, adaptive throttle, QPS limiting,
 * cost limiting, backpressure limiting, and concurrency scenarios.
 */
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

    /**
     * Mock MetaServiceRateLimiter that allows overriding available processors.
     */
    private static class MockMetaServiceRateLimiter extends MetaServiceRateLimiter {
        private final int processors;

        public MockMetaServiceRateLimiter(int processors) {
            super();
            this.processors = processors;
        }

        @Override
        protected int getAvailableProcessors() {
            return processors;
        }
    }

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
    public void testIsConfigChangedRateLimitEnabledToggle() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // true -> true
        Config.meta_service_rpc_rate_limit_enabled = true;
        Assert.assertFalse(limiter.isConfigChanged());

        // true -> false
        Config.meta_service_rpc_rate_limit_enabled = false;
        Assertions.assertTrue(limiter.isConfigChanged());

        // false -> false
        Config.meta_service_rpc_rate_limit_enabled = false;
        Assertions.assertFalse(limiter.isConfigChanged());

        // false -> true
        Config.meta_service_rpc_rate_limit_enabled = true;
        Assertions.assertTrue(limiter.isConfigChanged());
    }

    @Test
    public void testIsConfigChangedRateLimitEnabledToggle2() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // true -> true
        Config.meta_service_rpc_rate_limit_enabled = true;
        Assert.assertFalse(limiter.isConfigChanged());

        // true -> false
        Config.meta_service_rpc_rate_limit_enabled = false;
        Assertions.assertTrue(limiter.isConfigChanged());

        // false -> false
        Config.meta_service_rpc_rate_limit_enabled = false;
        Assertions.assertFalse(limiter.isConfigChanged());

        // false -> true
        Config.meta_service_rpc_rate_limit_enabled = true;
        Assertions.assertTrue(limiter.isConfigChanged());
    }

    @Test
    public void testIsConfigChanged_RateLimitEnabledToggle() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_enabled = false;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Initial state - should not detect change after construction
        Assert.assertFalse(limiter.isConfigChanged());

        // Change rate limit enabled
        Config.meta_service_rpc_rate_limit_enabled = false;
        Assertions.assertTrue(limiter.isConfigChanged());
    }

    @Test
    public void testIsConfigChanged_AdaptiveThrottleEnabledToggle() {
        Config.meta_service_rpc_rate_limit_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_enabled = true;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Initial state
        Assert.assertFalse(limiter.isConfigChanged());

        // Change adaptive throttle enabled
        Config.meta_service_rpc_adaptive_throttle_enabled = false;
        Assertions.assertTrue(limiter.isConfigChanged());
    }

    @Test
    public void testIsConfigChanged_DefaultQpsChange() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_enabled = false;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Initial state
        Assert.assertFalse(limiter.reloadConfig());

        // Change default QPS
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 20;
        Assertions.assertTrue(limiter.reloadConfig());
    }

    @Test
    public void testIsConfigChanged_MaxWaitRequestNumChange() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_enabled = false;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Initial state
        Assert.assertFalse(limiter.reloadConfig());

        // Change max wait request num
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 200;
        Assertions.assertTrue(limiter.reloadConfig());
    }

    @Test
    public void testIsConfigChanged_QpsConfigChange() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_enabled = false;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:10";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Initial state
        Assert.assertFalse(limiter.reloadConfig());

        // Change QPS config
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:20";
        Assertions.assertTrue(limiter.reloadConfig());
    }

    @Test
    public void testIsConfigChanged_CostConfigChange() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_enabled = false;
        Config.meta_service_rpc_cost_limit_per_core_config = "method1:10";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Initial state
        Assert.assertFalse(limiter.reloadConfig());

        // Change cost config
        Config.meta_service_rpc_cost_limit_per_core_config = "method1:20";
        Assertions.assertTrue(limiter.reloadConfig());
    }

    @Test
    public void testIsConfigChanged_AdaptiveThrottleMethodsChange() {
        Config.meta_service_rpc_rate_limit_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_methods = "method1,method2";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Initial state
        Assert.assertFalse(limiter.reloadConfig());

        // Change adaptive throttle methods
        Config.meta_service_rpc_adaptive_throttle_methods = "method1,method3";
        Assertions.assertTrue(limiter.reloadConfig());
    }

    @Test
    public void testIsConfigChanged_BothDisabledNoOtherCheck() {
        Config.meta_service_rpc_rate_limit_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_enabled = false;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Initial state
        Assert.assertFalse(limiter.reloadConfig());

        // Change other configs when both are disabled - should NOT detect change
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 999;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 999;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:999";
        Config.meta_service_rpc_cost_limit_per_core_config = "method1:999";

        // When both enabled flags are false, other config changes should be ignored
        Assert.assertFalse(limiter.reloadConfig());
    }

    // =========================================================================
    // Test: reloadAdaptiveThrottleConfig() method
    // =========================================================================

    @Test
    public void testReloadAdaptiveThrottleConfig_Disable() {
        Config.meta_service_rpc_rate_limit_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_methods = "method1,method2";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Enable adaptive throttle first
        limiter.reloadConfig();

        // Get the adaptiveThrottleMethods via reflection for verification
        Set<String> adaptiveMethods = getAdaptiveThrottleMethods(limiter);
        Assert.assertTrue(adaptiveMethods.contains("method1"));
        Assert.assertTrue(adaptiveMethods.contains("method2"));

        // Disable adaptive throttle
        Config.meta_service_rpc_adaptive_throttle_enabled = false;
        limiter.reloadConfig();

        adaptiveMethods = getAdaptiveThrottleMethods(limiter);
        Assert.assertTrue(adaptiveMethods.isEmpty());
    }

    @Test
    public void testReloadAdaptiveThrottleConfig_ParseMethods() {
        Config.meta_service_rpc_rate_limit_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_methods = "getVersion, beginTxn, commitTxn";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        limiter.reloadConfig();

        Set<String> adaptiveMethods = getAdaptiveThrottleMethods(limiter);
        Assert.assertEquals(3, adaptiveMethods.size());
        Assert.assertTrue(adaptiveMethods.contains("getVersion"));
        Assert.assertTrue(adaptiveMethods.contains("beginTxn"));
        Assert.assertTrue(adaptiveMethods.contains("commitTxn"));
    }

    @Test
    public void testReloadAdaptiveThrottleConfig_ParseMethodsWithSpaces() {
        Config.meta_service_rpc_rate_limit_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_methods = " method1 , method2 , method3 ";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        limiter.reloadConfig();

        Set<String> adaptiveMethods = getAdaptiveThrottleMethods(limiter);
        Assert.assertEquals(3, adaptiveMethods.contains("method1"));
        Assert.assertTrue(adaptiveMethods.contains("method1"));
        Assert.assertTrue(adaptiveMethods.contains("method2"));
        Assert.assertTrue(adaptiveMethods.contains("method3"));
    }

    @Test
    public void testReloadAdaptiveThrottleConfig_EmptyMethods() {
        Config.meta_service_rpc_rate_limit_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_methods = "";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        limiter.reloadConfig();

        Set<String> adaptiveMethods = getAdaptiveThrottleMethods(limiter);
        Assert.assertTrue(adaptiveMethods.isEmpty());
    }

    @Test
    public void testReloadAdaptiveThrottleConfig_NullMethods() {
        Config.meta_service_rpc_rate_limit_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_methods = null;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        limiter.reloadConfig();

        Set<String> adaptiveMethods = getAdaptiveThrottleMethods(limiter);
        Assert.assertTrue(adaptiveMethods.isEmpty());
    }

    // =========================================================================
    // Test: reloadRateLimiterConfig() method
    // =========================================================================

    @Test
    public void testReloadRateLimiterConfig_DisableClearsAll() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:10";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        Assertions.assertDoesNotThrow(() -> limiter.acquire("method1", 0));

        Map<String, Integer> qpsConfig = limiter.getMethodQpsConfig();
        Assert.assertEquals(1, qpsConfig.size());

        // Disable rate limiter
        Config.meta_service_rpc_rate_limit_enabled = false;
        limiter.reloadConfig();

        qpsConfig = limiter.getMethodQpsConfig();
        Assert.assertEquals(0, qpsConfig.size());
    }

    @Test
    public void testReloadRateLimiterConfig_ParseQpsConfig() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:20;method2:30";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        limiter.reloadConfig();

        Map<String, Integer> qpsConfig = limiter.getMethodQpsConfig();
        Assert.assertEquals(20, (int) qpsConfig.get("method1"));
        Assert.assertEquals(30, (int) qpsConfig.get("method2"));
    }

    @Test
    public void testReloadRateLimiterConfig_ParseCostConfig() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_cost_limit_per_core_config = "costMethod:50;costMethod2:100";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        limiter.reloadConfig();

        Map<String, Integer> costConfig = limiter.getMethodCostConfig();
        Assert.assertEquals(50, (int) costConfig.get("costMethod"));
        Assert.assertEquals(100, (int) costConfig.get("costMethod2"));
    }

    // =========================================================================
    // Test: acquire() method
    // =========================================================================

    @Test
    public void testAcquire_BothDisabled() {
        Config.meta_service_rpc_rate_limit_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_enabled = false;
        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
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
        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

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
        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
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
            Assert.assertEquals(90, limiter.getBackpressureQpsLimiters().get("method1").getRateLimiter().getRate());
        }
    }

    @Test
    public void testAcquire_BothEnabled() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;
        Config.meta_service_rpc_adaptive_throttle_methods = "method1";
        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        MetaServiceAdaptiveThrottle throttle = Mockito.mock(MetaServiceAdaptiveThrottle.class);
        Mockito.when(MetaServiceAdaptiveThrottle.getInstance()).thenReturn(throttle);
        Mockito.when(throttle.getFactor()).thenReturn(0.9);

        // qps enabled, cost disabled
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 1)));
        // Returns false because method1 is not in Config.meta_service_rpc_cost_limit_per_core_config
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(1, limiter.getQpsLimiters().size());
        Assert.assertEquals(0, limiter.getCostLimiters().size());
        Assert.assertEquals(1, limiter.getBackpressureQpsLimiters().size());

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
        Assert.assertEquals(5, limiter.getCostLimiters().size());
        limiter.release("method1", 5);
    }

    /*@Test
    public void testAcquire_CostLimitEnabled() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_cost_limit_per_core_config = "costMethod:10";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire with cost - should succeed
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("costMethod", 5)));
        Assert.assertTrue(acquired.get());

        // Release
        limiter.release("costMethod", 5);
    }*/

    /*@Test
    public void testAcquire_CostLimitExceed() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_cost_limit_per_core_config = "costMethod:5";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire with cost exceeding limit - should throw
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertThrows(RpcRateLimitException.class,
                () -> limiter.acquire("costMethod", 10));
    }*/

    // =========================================================================
    // Test: release() method
    // =========================================================================

    @Test
    public void testRelease_Basic() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_cost_limit_per_core_config = "releaseMethod:10";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire
        Assertions.assertDoesNotThrow(() -> limiter.acquire("releaseMethod", 5));

        // Release - should not throw
        Assertions.assertDoesNotThrow(() -> limiter.release("releaseMethod", 5));
    }

    @Test
    public void testRelease_NonExistentMethod() {
        Config.meta_service_rpc_rate_limit_enabled = true;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Release non-existent method - should not throw
        Assertions.assertDoesNotThrow(() -> limiter.release("nonExistent", 5));
    }

    @Test
    public void testRelease_NegativeCost() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_cost_limit_per_core_config = "negMethod:10";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire
        Assertions.assertDoesNotThrow(() -> limiter.acquire("negMethod", 5));

        // Release with negative cost - should not throw
        Assertions.assertDoesNotThrow(() -> limiter.release("negMethod", -1));
    }

    // =========================================================================
    // Test: getRequestCost() method
    // =========================================================================

    /*@Test
    public void testGetRequestCost_GetVersionWithBatchMode() {
        Config.meta_service_rpc_cost_clamped_to_limit_enabled = false;

        Cloud.GetVersionRequest request = Cloud.GetVersionRequest.newBuilder()
                .setBatchMode(true)
                .addDbIds(1)
                .addDbIds(2)
                .addDbIds(3)
                .build();

        int cost = MetaServiceRateLimiter.getRequestCost("getVersion", request);
        Assert.assertEquals(3, cost);
    }

    @Test
    public void testGetRequestCost_GetVersionWithoutBatchMode() {
        Cloud.GetVersionRequest request = Cloud.GetVersionRequest.newBuilder()
                .setBatchMode(false)
                .build();

        int cost = MetaServiceRateLimiter.getRequestCost("getVersion", request);
        Assert.assertEquals(1, cost);
    }

    @Test
    public void testGetRequestCost_GetVersionNullRequest() {
        int cost = MetaServiceRateLimiter.getRequestCost("getVersion", null);
        Assert.assertEquals(1, cost);
    }

    @Test
    public void testGetRequestCost_GetVersionInvalidRequest() {
        int cost = MetaServiceRateLimiter.getRequestCost("getVersion", "not a GetVersionRequest");
        Assert.assertEquals(1, cost);
    }

    @Test
    public void testGetRequestCost_GetVersionClampedToLimit() {
        Config.meta_service_rpc_cost_clamped_to_limit_enabled = true;

        Cloud.GetVersionRequest request = Cloud.GetVersionRequest.newBuilder()
                .setBatchMode(true)
                .addDbIds(1)
                .addDbIds(2)
                .addDbIds(3)
                .addDbIds(4)
                .addDbIds(5)
                .build();

        int cost = MetaServiceRateLimiter.getRequestCost("getVersion", request);
        Assert.assertEquals(5, cost); // Should be clamped to limit (need to set cost limit first)
    }

    @Test
    public void testGetRequestCost_OtherMethod() {
        int cost = MetaServiceRateLimiter.getRequestCost("someOtherMethod", null);
        Assert.assertEquals(1, cost);
    }*/

    // =========================================================================
    // Test: setAdaptiveFactor() method
    // =========================================================================

    /*@Test
    public void testSetAdaptiveFactor_Basic() {
        Config.meta_service_rpc_rate_limit_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_methods = "method1,method2";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        limiter.reloadConfig();

        // setAdaptiveFactor should not throw
        Assertions.assertDoesNotThrow(() -> limiter.setAdaptiveFactor(0.5));
        Assertions.assertDoesNotThrow(() -> limiter.setAdaptiveFactor(1.0));
        Assertions.assertDoesNotThrow(() -> limiter.setAdaptiveFactor(0.1));
    }

    @Test
    public void testSetAdaptiveFactor_WithQpsLimiter() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_adaptive_throttle_methods = "adaptiveMethod";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Create the limiter first
        limiter.acquire("adaptiveMethod", 0);

        // setAdaptiveFactor should not throw
        Assertions.assertDoesNotThrow(() -> limiter.setAdaptiveFactor(0.5));
    }*/

    // =========================================================================
    // Test: reset() method
    // =========================================================================

    @Test
    public void testReset_Basic() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:10";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        Assertions.assertDoesNotThrow(() -> limiter.acquire("method1", 0));

        // Verify config is loaded
        Map<String, Integer> qpsConfig = limiter.getMethodQpsConfig();
        Assert.assertEquals(1, qpsConfig.size());

        // Reset
        // limiter.reset();

        // Verify config is cleared
        qpsConfig = limiter.getMethodQpsConfig();
        Assert.assertEquals(0, qpsConfig.size());
    }

    // =========================================================================
    // Test: getMethodQpsConfig() and getMethodCostConfig()
    // =========================================================================

    @Test
    public void testGetMethodQpsConfig() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "testQps:20";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        Map<String, Integer> qpsConfig = limiter.getMethodQpsConfig();
        Assert.assertEquals(20, (int) qpsConfig.get("testQps"));
    }

    @Test
    public void testGetMethodCostConfig() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_cost_limit_per_core_config = "testCost:30";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        Map<String, Integer> costConfig = limiter.getMethodCostConfig();
        Assert.assertEquals(30, (int) costConfig.get("testCost"));
    }

    // =========================================================================
    // Test: Integration scenarios
    // =========================================================================

    @Test
    public void testIntegration_RateLimitWithAdaptiveThrottle() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;
        Config.meta_service_rpc_adaptive_throttle_methods = "adaptiveMethod";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire should work with both rate limit and adaptive throttle
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("adaptiveMethod", 0)));
        Assert.assertFalse(acquired.get());
    }

    @Test
    public void testIntegration_ReloadConfigMultipleTimes() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_enabled = false;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // First reload
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 20;
        Assert.assertTrue(limiter.reloadConfig());

        // Second reload with same config - should return false
        Assert.assertFalse(limiter.reloadConfig());

        // Third reload with different config
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 30;
        Assert.assertTrue(limiter.reloadConfig());

        // Fourth reload with same config
        Assert.assertFalse(limiter.reloadConfig());
    }

    @Test
    public void testIntegration_ConcurrentAcquire() throws InterruptedException {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 1000; // High QPS for concurrency test
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    limiter.acquire("concurrentTest", 0);
                    successCount.incrementAndGet();
                } catch (RpcRateLimitException e) {
                    failCount.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startLatch.countDown();
        executor.shutdown();
        boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);

        Assert.assertTrue("Executor should terminate", terminated);
        Assert.assertEquals(threadCount, successCount.get());
        Assert.assertEquals(0, failCount.get());
    }

    @Test
    public void testIntegration_QpsAndCostTogether() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;
        Config.meta_service_rpc_cost_limit_per_core_config = "together:10";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire with both QPS and cost limiting
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("together", 5)));
        Assert.assertTrue(acquired.get());

        // Release
        limiter.release("together", 5);
    }

    // =========================================================================
    // Test: Edge cases
    // =========================================================================

    @Test
    public void testEdgeCase_EmptyQpsConfig() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 50;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        limiter.reloadConfig();

        Map<String, Integer> qpsConfig = limiter.getMethodQpsConfig();
        Assert.assertEquals(0, qpsConfig.size());
    }

    @Test
    public void testEdgeCase_NullQpsConfig() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 50;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = null;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        limiter.reloadConfig();

        Map<String, Integer> qpsConfig = limiter.getMethodQpsConfig();
        Assert.assertEquals(0, qpsConfig.size());
    }

    @Test
    public void testEdgeCase_InvalidQpsConfigFormat() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 50;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "invalidformat;another:bad:format";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        limiter.reloadConfig();

        // Should use default QPS for methods without valid config
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("someMethod", 0)));
        Assert.assertFalse(acquired.get());
    }

    @Test
    public void testEdgeCase_NegativeQpsInConfig() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 50;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "negative:-10";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        limiter.reloadConfig();

        Map<String, Integer> qpsConfig = limiter.getMethodQpsConfig();
        // Negative QPS should be treated as 0, so method should not be in config
        Assert.assertFalse(qpsConfig.containsKey("negative"));
    }

    @Test
    public void testEdgeCase_ZeroQpsInConfig() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 50;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "zero:0";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        limiter.reloadConfig();

        Map<String, Integer> qpsConfig = limiter.getMethodQpsConfig();
        // Zero QPS should not create a limiter
        Assert.assertFalse(qpsConfig.containsKey("zero"));
    }

    @Test
    public void testEdgeCase_InvalidCostConfigFormat() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_cost_limit_per_core_config = "invalidcost;bad:format";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        limiter.reloadConfig();

        Map<String, Integer> costConfig = limiter.getMethodCostConfig();
        // Invalid format should be ignored
        Assert.assertEquals(0, costConfig.size());
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    private Set<String> getAdaptiveThrottleMethods(MetaServiceRateLimiter limiter) {
        try {
            Field field = MetaServiceRateLimiter.class.getDeclaredField("adaptiveThrottleMethods");
            field.setAccessible(true);
            return (Set<String>) field.get(limiter);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get adaptiveThrottleMethods", e);
        }
    }
}
