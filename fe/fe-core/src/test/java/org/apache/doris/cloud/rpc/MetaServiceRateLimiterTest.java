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

import org.apache.doris.cloud.rpc.MetaServiceRateLimiter.MethodRateLimiter;
import org.apache.doris.common.Config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MetaServiceRateLimiterTest {
    private static final Logger LOG = LogManager.getLogger(MetaServiceRateLimiterTest.class);

    private boolean originalEnabled;
    private int originalDefaultQps;
    private long originalWaitTimeout;
    private int originalMaxWaiting;
    private String originalQpsConfig;

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
        originalMaxWaiting = Config.meta_service_rpc_rate_limit_max_waiting;
        originalQpsConfig = Config.meta_service_rpc_rate_limit_qps_per_core_config;
    }

    @After
    public void tearDown() {
        Config.meta_service_rpc_rate_limit_enabled = originalEnabled;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = originalDefaultQps;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = originalWaitTimeout;
        Config.meta_service_rpc_rate_limit_max_waiting = originalMaxWaiting;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = originalQpsConfig;
    }

    @Test
    public void testDisabledByDefault() {
        Config.meta_service_rpc_rate_limit_enabled = false;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        Assertions.assertDoesNotThrow (() -> {
            limiter.acquire("testMethod");
            limiter.release("testMethod");
        });
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
        Assertions.assertDoesNotThrow (() -> {
            limiter.acquire("method1");
            limiter.release("method1");
        });

        // Disable rate limiter
        Config.meta_service_rpc_rate_limit_enabled = false;

        // After disabling, acquire/release should still work (no-op)
        Assertions.assertDoesNotThrow(() -> {
            limiter.acquire("method1");
            limiter.release("method1");
        });
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
        Assertions.assertDoesNotThrow(() -> {
            limiter.acquire("testMethod");
            limiter.release("testMethod");
        });
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
        Assertions.assertDoesNotThrow(() -> {
            limiter.acquire("testMethod");
            limiter.release("testMethod");
            limiter.acquire("method1");
            limiter.release("method1");
        });
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
        Config.meta_service_rpc_rate_limit_max_waiting = 5;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";
        int maxWaitingFailCount = 0;
        int acquireFailCount = 0;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        for (int i = 0; i < 10; i++) {
            try {
                limiter.acquire("testWaitTimeout");
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
        Assert.assertEquals(4, method.getAllowWaiting());
    }

    @Test
    public void testParseQpsConfigNullConfig() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = null;

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        // Should use default QPS
        Assertions.assertDoesNotThrow(() -> {
            limiter.acquire("testMethod");
            limiter.release("testMethod");
        });
        Assert.assertEquals(0, limiter.getMethodQpsConfig().size());
        Assert.assertEquals(1, limiter.getMethodLimiters().size());
    }

    @Test
    public void testParseQpsConfigInvalidFormat() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        // Invalid format: no colon separator
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "invalidformat";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);
        // Should use default QPS
        Assertions.assertDoesNotThrow(() -> {
            limiter.acquire("testMethod");
            limiter.release("testMethod");
        });
        Assert.assertEquals(0, limiter.getMethodQpsConfig().size());
        Assert.assertEquals(1, limiter.getMethodLimiters().size());
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
        Assertions.assertDoesNotThrow(() -> {
            limiter.acquire("method1");
            limiter.release("method1");
            limiter.acquire("method2");
            limiter.release("method2");
            limiter.acquire("otherMethod");
            limiter.release("otherMethod");
        });
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
        Assertions.assertDoesNotThrow(() -> {
            limiter.acquire("method1");
            limiter.release("method1");
            limiter.acquire("method2");
            limiter.release("method2");
        });

        // Remove method1 from config by setting QPS to 0
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method2:20";

        // method1 should be removed, use default QPS
        Assertions.assertDoesNotThrow(() -> {
            limiter.acquire("method1");
            limiter.release("method1");
            limiter.acquire("method2");
            limiter.release("method2");
        });
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
        Assertions.assertDoesNotThrow(() -> {
            limiter.acquire("testMethod");
            limiter.release("testMethod");
        });
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
        Assertions.assertDoesNotThrow(() -> {
            limiter.acquire("method1");
            limiter.release("method1");
            limiter.acquire("otherMethod");
            limiter.release("otherMethod");
        });
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
        Assertions.assertDoesNotThrow(() -> {
            limiter.acquire("method1");
            limiter.release("method1");
        });
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
                    limiter.acquire("testMethod");
                    successCount.incrementAndGet();
                    limiter.release("testMethod");
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
        Config.meta_service_rpc_rate_limit_max_waiting = 99;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";

        MetaServiceRateLimiter limiter = new MockMetaServiceRateLimiter(1);

        // Initial acquire creates limiter with default QPS (10)
        Assertions.assertDoesNotThrow(() -> {
            limiter.acquire("method1");
        });
        Assert.assertEquals(1, limiter.getMethodLimiters().size());
        MethodRateLimiter method1 = limiter.getMethodLimiters().get("method1");
        Assert.assertNotNull(method1);
        Assert.assertEquals(98, method1.getAllowWaiting());

        // Change config - this should trigger reload on next acquire
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "method1:20";
        Assert.assertTrue(limiter.reloadConfig());
        Assert.assertEquals(1, limiter.getMethodLimiters().size());
        method1 = limiter.getMethodLimiters().get("method1");
        Assert.assertNotNull(method1);
        Assert.assertEquals(20, (int) method1.getRateLimiter().getRate());
        Assert.assertEquals(98, method1.getAllowWaiting());

        // Acquire should trigger reloadConfig and create/update limiter
        Assertions.assertDoesNotThrow(() -> {
            limiter.release("method1");
        });
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
        Assertions.assertDoesNotThrow(() -> {
            limiter.acquire("method1");
        });
        Assert.assertEquals(1, limiter.getMethodLimiters().size());

        // Disable rate limiter
        Config.meta_service_rpc_rate_limit_enabled = false;
        limiter.reloadConfig();

        // Release after disable should be a no-op
        // Should not throw exception
        Assertions.assertDoesNotThrow(() -> {
            limiter.release("method1");
        });

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
        Assertions.assertDoesNotThrow(() -> {
            limiter.acquire("method1");
        });
        Assert.assertEquals(1, limiter.getMethodLimiters().size());

        // Directly release after disable without calling reloadConfig
        Config.meta_service_rpc_rate_limit_enabled = false;

        // Release should check enabled first and skip
        // This should not throw because release() checks enabled before accessing limiter
        Assertions.assertDoesNotThrow(() -> {
            limiter.release("method1");
        });

        // Limiter still exists but release was skipped
        Assert.assertEquals(1, limiter.getMethodLimiters().size());

        Assertions.assertDoesNotThrow(() -> {
            limiter.acquire("method1");
            limiter.release("method1");
        });
        Assert.assertEquals(0, limiter.getMethodLimiters().size());
    }
}
