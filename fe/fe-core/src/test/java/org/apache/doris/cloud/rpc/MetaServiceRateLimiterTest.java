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
import org.apache.doris.cloud.rpc.MetaServiceRateLimiter.CostLimiter;
import org.apache.doris.cloud.rpc.MetaServiceRateLimiter.MethodRateLimiter;
import org.apache.doris.common.Config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MetaServiceRateLimiterTest {
    private static final Logger LOG = LogManager.getLogger(MetaServiceRateLimiterTest.class);

    private boolean originalEnabled;
    private int originalDefaultQps;
    private long originalWaitTimeout;
    private int originalMaxWaiting;
    private String originalQpsConfig;
    private String originalCostConfig;

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
        originalEnabled = Config.meta_service_rpc_rate_limit_enabled;
        originalDefaultQps = Config.meta_service_rpc_rate_limit_default_qps_per_core;
        originalWaitTimeout = Config.meta_service_rpc_rate_limit_wait_timeout_ms;
        originalMaxWaiting = Config.meta_service_rpc_rate_limit_max_waiting_request_num;
        originalQpsConfig = Config.meta_service_rpc_rate_limit_qps_per_core_config;
        originalCostConfig = Config.meta_service_rpc_cost_limit_per_core_config;
    }

    @After
    public void tearDown() {
        Config.meta_service_rpc_rate_limit_enabled = originalEnabled;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = originalDefaultQps;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = originalWaitTimeout;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = originalMaxWaiting;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = originalQpsConfig;
        Config.meta_service_rpc_cost_limit_per_core_config = originalCostConfig;
    }

    @Test
    public void testDisabledByDefault() {
        Config.meta_service_rpc_rate_limit_enabled = false;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("testMethod", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(0, limiter.getMethodQpsConfig().size());
        Assert.assertEquals(0, limiter.getMethodLimiters().size());
    }

    @Test
    public void testDisableRateLimiter() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:10";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());

        // Disable rate limiter
        Config.meta_service_rpc_rate_limit_enabled = false;
        // After disabling, acquire should return false
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(0, limiter.getMethodQpsConfig().size());
        Assert.assertEquals(0, limiter.getMethodLimiters().size());
    }

    @Test
    public void testBasicAcquireRelease() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(8);
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("testMethod", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(0, limiter.getMethodQpsConfig().size());
        Assert.assertEquals(1, limiter.getMethodLimiters().size());
        MethodRateLimiter testMethod = limiter.getMethodLimiters().get("testMethod");
        Assert.assertNotNull(testMethod);
        Assert.assertEquals(800, (int) testMethod.getRateLimiter().getRate());
    }

    @Test
    public void testMultipleProcessors() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:20";

        // With 4 processors, QPS should be 40 (10 * 4)
        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(4);
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("testMethod", 0)));
        Assert.assertFalse(acquired.get());
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(1, limiter.getMethodQpsConfig().size());
        Assert.assertEquals(2, limiter.getMethodLimiters().size());
        MethodRateLimiter testMethod = limiter.getMethodLimiters().get("testMethod");
        Assert.assertNotNull(testMethod);
        Assert.assertEquals(40, (int) testMethod.getRateLimiter().getRate());
        MethodRateLimiter method1 = limiter.getMethodLimiters().get("method1");
        Assert.assertNotNull(method1);
        Assert.assertEquals(80, (int) method1.getRateLimiter().getRate());
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

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        for (int i = 0; i < 10; i++) {
            try {
                limiter.acquire("testWaitTimeout", 0);
            } catch (RpcRateLimitException e) {
                LOG.warn("i={}", i, e);
                if (e.getMessage().contains("too many waiting requests")) {
                    maxWaitingFailCount++;
                } else if (e.getMessage().contains("timeout for method")) {
                    acquireFailCount++;
                }
            }
        }
        Assert.assertEquals(0, maxWaitingFailCount);
        Assert.assertEquals(9, acquireFailCount);
        Assert.assertEquals(1, limiter.getMethodLimiters().size());
        MethodRateLimiter method = limiter.getMethodLimiters().get("testWaitTimeout");
        Assert.assertNotNull(method);
        Assert.assertEquals(5, method.getAllowWaiting());
    }

    @Test
    public void testParseQpsConfigInvalidFormat() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        // Invalid format: no colon separator
        String[] configs = {null, "invalidformat"};
        for (String config : configs) {
            Config.meta_service_rpc_rate_limit_qps_per_core_config = config;
            MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
            // Should use default QPS
            AtomicBoolean acquired = new AtomicBoolean(false);
            Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("testMethod", 0)));
            Assert.assertFalse(acquired.get());
            Assert.assertEquals(0, limiter.getMethodQpsConfig().size());
            Assert.assertEquals(1, limiter.getMethodLimiters().size());
        }
    }

    @Test
    public void testParseQpsConfigInvalidNumber() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        // Invalid QPS: not a number
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:invalid;method2:20";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        // Should use default QPS
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method2", 0)));
        Assert.assertFalse(acquired.get());
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("otherMethod", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(1, limiter.getMethodQpsConfig().size());
        Assert.assertEquals(3, limiter.getMethodLimiters().size());
    }

    @Test
    public void testMethodRemovalOnConfigReload() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:10;method2:20";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method2", 0)));
        Assert.assertFalse(acquired.get());

        // Remove method1 from config by setting QPS to 0
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method2:20";

        // method1 should be removed, use default QPS
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method2", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(1, limiter.getMethodQpsConfig().size());
        Assert.assertEquals(2, limiter.getMethodLimiters().size());
        MethodRateLimiter method1 = limiter.getMethodLimiters().get("method1");
        Assert.assertNotNull(method1);
        Assert.assertEquals(10, (int) method1.getRateLimiter().getRate());
        MethodRateLimiter method2 = limiter.getMethodLimiters().get("method2");
        Assert.assertNotNull(method2);
        Assert.assertEquals(20, (int) method2.getRateLimiter().getRate());
    }

    @Test
    public void testZeroQpsPerCore() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        // When default QPS is 0, limiter should be null (no rate limiting)
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("testMethod", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(0, limiter.getMethodLimiters().size());
    }

    @Test
    public void testNegativeQpsInConfig() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        // Negative QPS should be treated as 0
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:-5";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        // method1 should have 0 QPS (no limiter), otherMethod uses default
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("otherMethod", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(1, limiter.getMethodQpsConfig().size());
        Assert.assertEquals(1, limiter.getMethodLimiters().size());
        Assert.assertTrue(limiter.getMethodQpsConfig().containsKey("method1"));
        Assert.assertTrue(limiter.getMethodLimiters().containsKey("otherMethod"));
    }

    @Test
    public void testDoubleCheckLockingPattern() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:10";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        Assertions.assertFalse(limiter.reloadConfig());
        // Second reload with no config change should return early
        Assertions.assertFalse(limiter.reloadConfig());

        // Should still work normally
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(1, limiter.getMethodQpsConfig().size());
        Assert.assertEquals(1, limiter.getMethodLimiters().size());
    }

    @Test
    public void testConcurrentAccess() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 5000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";

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
                    boolean acquired = limiter.acquire("testMethod", 0);
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
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("Test interrupted", e);
        }
    }

    @Test
    public void testAcquireWithConfigChange() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 99;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Initial acquire creates limiter with default QPS (10)
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(1, limiter.getMethodLimiters().size());
        MethodRateLimiter method1 = limiter.getMethodLimiters().get("method1");
        Assert.assertNotNull(method1);
        Assert.assertEquals(99, method1.getAllowWaiting());

        // Change config - this should trigger reload on next acquire
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:20";
        Assert.assertTrue(limiter.reloadConfig());
        Assert.assertEquals(1, limiter.getMethodLimiters().size());
        method1 = limiter.getMethodLimiters().get("method1");
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

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire to create a limiter
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(1, limiter.getMethodLimiters().size());

        // Disable rate limiter
        Config.meta_service_rpc_rate_limit_enabled = false;
        limiter.reloadConfig();

        // Release after disable should be a no-op
        // Should not throw exception
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());

        // Limiter map should be cleared after reloadConfig
        Assert.assertEquals(0, limiter.getMethodLimiters().size());
    }

    @Test
    public void testReleaseWithExistingLimiterAfterDisable() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire to create a limiter
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(1, limiter.getMethodLimiters().size());

        // Directly release after disable without calling reloadConfig
        Config.meta_service_rpc_rate_limit_enabled = false;

        // Release should check enabled first and skip
        // This should not throw because release() checks enabled before accessing limiter
        Assertions.assertDoesNotThrow(() -> limiter.release("method1", 0));

        // Limiter still exists but release was skipped
        Assert.assertEquals(1, limiter.getMethodLimiters().size());

        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("method1", 0)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(0, limiter.getMethodLimiters().size());
    }

    @Test
    public void testCostLimitAcquisitionAndFailure() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_cost_limit_per_core_config = "costMethod:5";
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire cost 3 - should succeed
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("costMethod", 3)));
        Assert.assertTrue(acquired.get());
        CostLimiter costLimiter = limiter.getMethodLimiters().get("costMethod").getCostLimiter();
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
        Assert.assertTrue(exception.getMessage().contains("cost limit"));
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

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire cost 1 - should succeed
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("reloadMethod", 1)));
        Assert.assertTrue(acquired.get());
        CostLimiter costLimiter = limiter.getMethodLimiters().get("reloadMethod").getCostLimiter();
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
    public void testCostConfigParsingIgnoresInvalidEntries() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_cost_limit_per_core_config = "goodMethod:5;;bad;bad2:notnum;zero:0";
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire goodMethod cost 4 - should succeed
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("goodMethod", 4)));
        Assert.assertTrue(acquired.get());
        CostLimiter costLimiter = limiter.getMethodLimiters().get("goodMethod").getCostLimiter();
        Assert.assertNotNull(costLimiter);
        Assert.assertEquals(4, costLimiter.getCurrentCost());

        limiter.release("goodMethod", 4);
        Assert.assertEquals(0, costLimiter.getCurrentCost());

        // Acquire zero cost 1 - should succeed (cost 0 means no cost limit)
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("zero", 1)));
        Assert.assertFalse(acquired.get());

        // Acquire bad2 cost 1 - should succeed (invalid format treated as default 0)
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("bad2", 1)));
        Assert.assertFalse(acquired.get());

        Assert.assertTrue(limiter.getMethodCostConfig().containsKey("goodMethod"));
        Assert.assertTrue(limiter.getMethodCostConfig().containsKey("zero"));
        Assert.assertFalse(limiter.getMethodCostConfig().containsKey("bad2"));
    }

    @Test
    public void testCostLimitWithDisableEnabledToggle() {
        // Step 1: Enable rate limit, set cost limit to 5
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_cost_limit_per_core_config = "testCostMethod:5";
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Step 2: Acquire cost 4 (should succeed, limit is 5)
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("testCostMethod", 4)));
        Assert.assertTrue(acquired.get());
        // Verify current cost is 4
        CostLimiter costLimiter = limiter.getMethodLimiters().get("testCostMethod").getCostLimiter();
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

        // Release cost 4 (works because limiter still exists)
        limiter.release("testCostMethod", 4);
        Assert.assertEquals(4, costLimiter.getCurrentCost());
        Assert.assertNull(limiter.getMethodLimiters().get("testCostMethod"));
    }

    @Test
    public void testBothRateAndCostLimiterBasic() {
        // Set both QPS and cost limits
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_cost_limit_per_core_config = "bothMethod:5";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire with cost within both limits
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("bothMethod", 3)));
        Assert.assertTrue(acquired.get());

        // Verify cost limiter is created and has correct values
        MethodRateLimiter methodLimiter = limiter.getMethodLimiters().get("bothMethod");
        Assert.assertNotNull(methodLimiter);
        Assert.assertNotNull(methodLimiter.getRateLimiter());
        Assert.assertEquals(10, (int) methodLimiter.getRateLimiter().getRate());

        CostLimiter costLimiter = methodLimiter.getCostLimiter();
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

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire cost 2 - should succeed
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("costFirst", 2)));
        Assert.assertTrue(acquired.get());
        CostLimiter costLimiter = limiter.getMethodLimiters().get("costFirst").getCostLimiter();
        Assert.assertEquals(2, costLimiter.getCurrentCost());

        // Acquire cost 2 again - total cost would be 4, limit is 3, should fail
        RpcRateLimitException exception = Assertions.assertThrows(RpcRateLimitException.class,
                () -> limiter.acquire("costFirst", 2));
        Assert.assertTrue(exception.getMessage().contains("cost limit"));

        // Verify QPS rate limiter was never touched (cost blocked first)
        MethodRateLimiter methodLimiter = limiter.getMethodLimiters().get("costFirst");
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

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

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

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire - uses both QPS slot and cost
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("releaseTest", 3)));
        Assert.assertTrue(acquired.get());
        CostLimiter costLimiter = limiter.getMethodLimiters().get("releaseTest").getCostLimiter();
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

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Initial acquire
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("reloadBoth", 3)));
        Assert.assertTrue(acquired.get());

        MethodRateLimiter methodLimiter = limiter.getMethodLimiters().get("reloadBoth");
        Assert.assertEquals(10, (int) methodLimiter.getRateLimiter().getRate());
        Assert.assertEquals(3, methodLimiter.getCostLimiter().getCurrentCost());

        // Reload with new values
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "reloadBoth:20";
        Config.meta_service_rpc_cost_limit_per_core_config = "reloadBoth:8";
        limiter.reloadConfig();

        // Verify both were updated
        methodLimiter = limiter.getMethodLimiters().get("reloadBoth");
        Assert.assertEquals(20, (int) methodLimiter.getRateLimiter().getRate());

        // Cost limit increased, can acquire more
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("reloadBoth", 5)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(8, methodLimiter.getCostLimiter().getCurrentCost());

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

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Should still work with only cost limit (QPS is 0 so no rate limiting)
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("zeroQps", 3)));
        Assert.assertTrue(acquired.get());

        MethodRateLimiter methodLimiter = limiter.getMethodLimiters().get("zeroQps");
        // Rate limiter should be null since QPS is 0
        Assert.assertNull(methodLimiter.getRateLimiter());
        // But cost limiter should exist
        Assert.assertNotNull(methodLimiter.getCostLimiter());
        Assert.assertEquals(3, methodLimiter.getCostLimiter().getCurrentCost());

        // Release
        limiter.release("zeroQps", 3);
        Assert.assertEquals(0, methodLimiter.getCostLimiter().getCurrentCost());
    }

    @Test
    public void testCostAndQpsWithZeroMethodCost() {
        // Method-specific cost is 0 (disabled), but default QPS is set
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_cost_limit_per_core_config = "zeroCost:0";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Should work with only QPS limit (cost is 0)
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("zeroCost", 0)));
        Assert.assertFalse(acquired.get());

        MethodRateLimiter methodLimiter = limiter.getMethodLimiters().get("zeroCost");
        Assert.assertNotNull(methodLimiter.getRateLimiter());
        // Cost limiter should be null since cost config is 0
        Assert.assertNull(methodLimiter.getCostLimiter());

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

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

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

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Try to acquire cost greater than limit - should not require limit
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertThrows(RpcRateLimitException.class, () -> limiter.acquire("exceedCost", 10));
        Assert.assertFalse(acquired.get());

        // Verify cost limiter was created but current cost is 0 (10 > 5)
        MethodRateLimiter methodLimiter = limiter.getMethodLimiters().get("exceedCost");
        Assert.assertNotNull(methodLimiter);
        CostLimiter costLimiter = methodLimiter.getCostLimiter();
        Assert.assertNotNull(costLimiter);
        Assert.assertEquals(0, costLimiter.getCurrentCost());

        // Release - should only release if acquired
        if (acquired.get()) {
            limiter.release("exceedCost", 10);
        }
        Assert.assertEquals(0, costLimiter.getCurrentCost());
    }

    @Test
    public void testCostClampedToLimit() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 10;
        Config.meta_service_rpc_cost_limit_per_core_config = "getVersion:5";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        Mockito.when(MetaServiceRateLimiter.getInstance()).thenReturn(limiter);

        Cloud.GetVersionRequest.Builder builder = Cloud.GetVersionRequest.newBuilder().setBatchMode(true);
        for (int i = 0; i < 10; i++) {
            builder.addDbIds(i);
            builder.addTableIds(i);
        }
        int cost = limiter.getRequestCost("getVersion", builder.build());

        // Try to acquire cost greater than limit - should require limit (cost is clamped to 5)
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> limiter.acquire("getVersion", cost));
        Assert.assertTrue(acquired.get());

        // Verify cost limiter was created but current cost is limit (10 > 5)
        MethodRateLimiter methodLimiter = limiter.getMethodLimiters().get("getVersion");
        Assert.assertNotNull(methodLimiter);
        CostLimiter costLimiter = methodLimiter.getCostLimiter();
        Assert.assertNotNull(costLimiter);
        Assert.assertEquals(5, costLimiter.getCurrentCost());

        // Release - should only release if acquired
        if (acquired.get()) {
            limiter.release("getVersion", cost);
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

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

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

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Should fail immediately due to "too many waiting requests"
        RpcRateLimitException exception = Assertions.assertThrows(RpcRateLimitException.class,
                () -> limiter.acquire("zeroWait", 0));
        Assert.assertTrue(exception.getMessage().contains("too many waiting requests"));
    }

    @Test
    public void testBothQpsAndCostZero() {
        // Test that both QPS=0 and cost=0 means no rate limiting
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";
        Config.meta_service_rpc_cost_limit_per_core_config = "";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Should return false since both QPS and cost limiters are null
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("bothZero", 1)));
        Assert.assertFalse(acquired.get());

        // No method limiters should be created
        Assert.assertEquals(0, limiter.getMethodLimiters().size());
    }

    @Test
    public void testCostLimitExactBoundary() {
        // Test exact boundary: current=4, limit=5, acquire=1 should succeed, acquire=2 should fail
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_cost_limit_per_core_config = "boundary:5";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire cost 4 - should succeed
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("boundary", 4)));
        Assert.assertTrue(acquired.get());

        CostLimiter costLimiter = limiter.getMethodLimiters().get("boundary").getCostLimiter();
        Assert.assertEquals(4, costLimiter.getCurrentCost());

        // Acquire cost 1 - should succeed (4+1=5, exactly at limit)
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("boundary", 1)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(5, costLimiter.getCurrentCost());

        // Acquire cost 1 again - should fail (5+1 > 5)
        RpcRateLimitException exception = Assertions.assertThrows(RpcRateLimitException.class,
                () -> limiter.acquire("boundary", 1));
        Assert.assertTrue(exception.getMessage().contains("cost limit"));
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

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Acquire 3 times
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("accuracy", 2)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(2, limiter.getMethodLimiters().get("accuracy").getCostLimiter().getCurrentCost());

        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("accuracy", 3)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(5, limiter.getMethodLimiters().get("accuracy").getCostLimiter().getCurrentCost());

        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("accuracy", 5)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(10, limiter.getMethodLimiters().get("accuracy").getCostLimiter().getCurrentCost());

        // Release one
        limiter.release("accuracy", 3);
        Assert.assertEquals(7, limiter.getMethodLimiters().get("accuracy").getCostLimiter().getCurrentCost());

        // Release all
        limiter.release("accuracy", 2);
        limiter.release("accuracy", 5);
        Assert.assertEquals(0, limiter.getMethodLimiters().get("accuracy").getCostLimiter().getCurrentCost());
    }

    @Test
    public void testReloadRecreatesRemovedMethod() {
        // Test that re-adding a removed method works correctly
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 0;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_cost_limit_per_core_config = "recreate:5";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // First acquire
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("recreate", 1)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(1, limiter.getMethodLimiters().get("recreate").getCostLimiter().getCurrentCost());

        // Remove from config
        Config.meta_service_rpc_cost_limit_per_core_config = "";
        limiter.reloadConfig();

        // Method should be removed
        Assert.assertNull(limiter.getMethodLimiters().get("recreate"));

        // Add back to config
        Config.meta_service_rpc_cost_limit_per_core_config = "recreate:10";
        limiter.reloadConfig();

        // Should work again
        acquired.set(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("recreate", 5)));
        Assert.assertTrue(acquired.get());
        Assert.assertEquals(5, limiter.getMethodLimiters().get("recreate").getCostLimiter().getCurrentCost());

        limiter.release("recreate", 5);
        limiter.release("recreate", 1);
    }

    @Test
    public void test() throws RpcRateLimitException {
        MethodRateLimiter realMethodRateLimiter = new MethodRateLimiter("testMethod", 8, 1, 7);
        // MethodRateLimiter methodRateLimiter = Mockito.mock(MethodRateLimiter.class);
        MethodRateLimiter methodRateLimiter = Mockito.spy(realMethodRateLimiter);
        Mockito.doThrow(new RpcRateLimitException("QPS limit exceeded"))
                .when(methodRateLimiter)
                .acquireQpsRateLimit();
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertThrows(RpcRateLimitException.class, () -> acquired.set(methodRateLimiter.acquire(3)));
        Assert.assertFalse(acquired.get());
        Assert.assertEquals(0, methodRateLimiter.getCostLimiter().getCurrentCost());
    }

    /**
     * Test that when cost limit passes but rate limiter (QPS) fails,
     * the cost limiter's current cost should be released to 0.
     *
     * This test uses mock to simulate rate limiter failure.
     */
    /*@Test
    public void testCostPassesButRateLimiterFails() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 100;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_cost_limit_per_core_config = "rateFailMethod:100";

        // Use atomic counter: first call succeeds, second call rate limiter throws
        final AtomicInteger callCount = new AtomicInteger(0);

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        Mockito.when(limiter.acquire("rateFailMethod", 10)).thenThrow(invocation -> {
            if (callCount.incrementAndGet() == 1) {
                // First call succeeds
                return true;
            } else {
                // Second call simulates rate limiter failure
                throw new RpcRateLimitException("Rate limiter failed for method: rateFailMethod");
            }
        });
        *//* {
            @Override
            protected MethodRateLimiter getMethodLimiter(String methodName) {
                return methodLimiters.compute(methodName, (name, limiter) -> {
                    if (limiter != null) {
                        return limiter;
                    }
                    int qps = getMethodTotalQps(name, Config.meta_service_rpc_rate_limit_default_qps_per_core);
                    int costLimit = getMethodTotalCostLimit(name);
                    if (qps > 0 || costLimit > 0) {
                        // First call uses normal limiter, subsequent calls use mock
                        if (callCount.incrementAndGet() == 1) {
                            return new MethodRateLimiter(name,
                                    Config.meta_service_rpc_rate_limit_max_waiting_request_num, qps, costLimit);
                        } else {
                            return new MethodRateLimiter(name,
                                    Config.meta_service_rpc_rate_limit_max_waiting_request_num, qps, costLimit) {
                                @Override
                                void acquireQpsRateLimit() throws RpcRateLimitException {
                                    throw new RpcRateLimitException("Rate limiter failed for method: " + name);
                                }
                            };
                        }
                    }
                    return null;
                });
            }
        };*//*

        // First acquire - should succeed
        AtomicBoolean acquired = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> acquired.set(limiter.acquire("rateFailMethod", 10)));
        Assert.assertTrue(acquired.get());

        CostLimiter costLimiter = limiter.getMethodLimiters().get("rateFailMethod").getCostLimiter();
        Assert.assertEquals(10, costLimiter.getCurrentCost());

        // Second acquire - cost passes but rate limiter throws
        acquired.set(false);
        RpcRateLimitException exception = Assertions.assertThrows(RpcRateLimitException.class,
                () -> limiter.acquire("rateFailMethod", 10));

        Assert.assertTrue(exception.getMessage().contains("Rate limiter failed"));

        // Key assertion: cost should be released to 0
        Assert.assertEquals("Cost should be released to 0 when rate limiter fails",
                0, costLimiter.getCurrentCost());

        // Release first acquisition
        limiter.release("rateFailMethod", 10);
        Assert.assertEquals(0, costLimiter.getCurrentCost());
    }*/
}
