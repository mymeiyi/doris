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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RpcRateLimiterTest2 {
    private static final Logger LOG = LogManager.getLogger(RpcRateLimiterTest2.class);

    private long originalWaitTimeout;
    private int originalMaxWaiting;

    @Before
    public void setUp() {
        originalWaitTimeout = Config.meta_service_rpc_rate_limit_wait_timeout_ms;
        originalMaxWaiting = Config.meta_service_rpc_rate_limit_max_waiting_request_num;
    }

    @After
    public void tearDown() {
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = originalWaitTimeout;
        Config.meta_service_rpc_rate_limit_max_waiting_request_num = originalMaxWaiting;
    }

    // ==================== QpsLimiter Tests ====================

    @Test
    public void testQpsLimiterConstructor() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, qps);
        Assert.assertEquals(methodName, limiter.methodName);
        Assert.assertEquals(maxWaitRequestNum, limiter.getMaxWaitRequestNum());
        Assert.assertEquals(qps, limiter.qps);
        Assert.assertNotNull(limiter.getRateLimiter());
        Assert.assertEquals(qps, limiter.getRateLimiter().getRate(), 0.001);
        Assert.assertEquals(maxWaitRequestNum, limiter.getAllowWaiting());

        // Invalid Arguments
        Assert.assertThrows(IllegalArgumentException.class, () -> new RpcRateLimiter.QpsLimiter(methodName, 10, 0));
        Assert.assertThrows(IllegalArgumentException.class, () -> new RpcRateLimiter.QpsLimiter(methodName, 10, -1));
        Assert.assertThrows(IllegalArgumentException.class, () -> new RpcRateLimiter.QpsLimiter(methodName, 0, 10));
        Assert.assertThrows(IllegalArgumentException.class, () -> new RpcRateLimiter.QpsLimiter(methodName, -1, 10));
    }

    @Test
    public void testQpsLimiterAcquireSuccess() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 1000;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, qps);
        Assertions.assertDoesNotThrow(() -> limiter.acquire());
    }

    @Test
    public void testQpsLimiterAcquireWaitingQueueFull() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 2;
        int qps = 1;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, qps);

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    limiter.acquire();
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
            LOG.error("Test interrupted", e);
            Assert.fail("Test was interrupted: " + e.getMessage());
        }
        int successes = successCount.get();
        int failures = failCount.get();
        Assert.assertEquals("Total results should match thread count", threadCount, successes + failures);
        // TODO
        LOG.info("Acquire results: {} successes, {} failures", successes, failures);
    }

    @Test
    public void testQpsLimiterUpdateQps() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int initialQps = 100;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, initialQps);
        Assert.assertEquals(initialQps, limiter.qps);
        Assert.assertEquals(maxWaitRequestNum, limiter.getAllowWaiting());

        // Update with same values - do nothing
        limiter.updateQps(maxWaitRequestNum, initialQps);
        Assert.assertEquals(initialQps, limiter.qps);
        Assert.assertEquals(maxWaitRequestNum, limiter.getMaxWaitRequestNum());
        Assert.assertEquals(maxWaitRequestNum, limiter.getAllowWaiting());

        // increase QPS
        int newQps = 200;
        limiter.updateQps(maxWaitRequestNum, newQps);
        Assert.assertEquals(newQps, limiter.qps);
        Assert.assertEquals(newQps, limiter.getRateLimiter().getRate(), 0.001);

        // decrease QPS back to initial
        limiter.updateQps(maxWaitRequestNum, initialQps);
        Assert.assertEquals(initialQps, limiter.qps);
        Assert.assertEquals(initialQps, limiter.getRateLimiter().getRate(), 0.001);

        // zero qps
        Assertions.assertThrows(IllegalArgumentException.class, () -> limiter.updateQps(maxWaitRequestNum, 0));
        // negative qps
        Assertions.assertThrows(IllegalArgumentException.class, () -> limiter.updateQps(maxWaitRequestNum, -1));
    }

    @Test
    public void testQpsLimiterUpdateMaxWait() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, qps);
        Assert.assertEquals(maxWaitRequestNum, limiter.getMaxWaitRequestNum());
        Assert.assertEquals(maxWaitRequestNum, limiter.getAllowWaiting());

        // same value - should do nothing
        limiter.updateQps(maxWaitRequestNum, qps);
        Assert.assertEquals(maxWaitRequestNum, limiter.getMaxWaitRequestNum());
        Assert.assertEquals(maxWaitRequestNum, limiter.getAllowWaiting());

        // increase maxWaitRequestNum
        int newMaxWait = 20;
        limiter.updateQps(newMaxWait, qps);
        Assert.assertEquals(newMaxWait, limiter.getMaxWaitRequestNum());
        Assert.assertEquals(newMaxWait, limiter.getAllowWaiting());

        // decrease maxWaitRequestNum back to initial
        limiter.updateQps(maxWaitRequestNum, qps);
        Assert.assertEquals(maxWaitRequestNum, limiter.getMaxWaitRequestNum());
        Assert.assertEquals(maxWaitRequestNum, limiter.getAllowWaiting());

        // zero maxWaitRequestNum
        Assert.assertThrows(IllegalArgumentException.class, () -> limiter.updateQps(0, qps));
        // negative maxWaitRequestNum
        Assert.assertThrows(IllegalArgumentException.class, () -> limiter.updateQps(-1, qps));
    }

    // ==================== BackpressureQpsLimiter Tests ====================

    @Test
    public void testBackpressureQpsLimiterConstructor() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;
        double factor = 1.0;

        RpcRateLimiter.BackpressureQpsLimiter limiter =
                new RpcRateLimiter.BackpressureQpsLimiter(methodName, maxWaitRequestNum, qps, factor);

        Assert.assertEquals(methodName, limiter.methodName);
        // Effective QPS should be qps * factor = 100 * 1.0 = 100
        Assert.assertEquals(100, limiter.qps);
    }

    @Test
    public void testBackpressureQpsLimiterApplyFactorOne() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;
        double factor = 1.0;

        RpcRateLimiter.BackpressureQpsLimiter limiter =
                new RpcRateLimiter.BackpressureQpsLimiter(methodName, maxWaitRequestNum, qps, factor);

        // Apply factor 1.0 - QPS should remain 100
        limiter.applyFactor(factor);

        Assert.assertEquals(100, limiter.qps);
        Assert.assertEquals(100, limiter.getRateLimiter().getRate(), 0.001);
    }

    @Test
    public void testBackpressureQpsLimiterApplyFactorHalf() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;
        double factor = 0.5;

        RpcRateLimiter.BackpressureQpsLimiter limiter =
                new RpcRateLimiter.BackpressureQpsLimiter(methodName, maxWaitRequestNum, qps, factor);

        // Apply factor 0.5 - effective QPS should be qps * 0.5 = 50
        Assert.assertEquals(50, limiter.qps);
        Assert.assertEquals(50, limiter.getRateLimiter().getRate(), 0.001);
    }

    @Test
    public void testBackpressureQpsLimiterApplyFactorQuarter() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;
        double factor = 0.25;

        RpcRateLimiter.BackpressureQpsLimiter limiter =
                new RpcRateLimiter.BackpressureQpsLimiter(methodName, maxWaitRequestNum, qps, factor);

        // Apply factor 0.25 - effective QPS should be 25
        Assert.assertEquals(25, limiter.qps);
    }

    @Test
    public void testBackpressureQpsLimiterApplyFactorZero() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;
        double factor = 0.0;

        RpcRateLimiter.BackpressureQpsLimiter limiter =
                new RpcRateLimiter.BackpressureQpsLimiter(methodName, maxWaitRequestNum, qps, factor);

        // Apply factor 0.0 - effective QPS should be at least 1 (Math.max(1, ...))
        Assert.assertEquals(1, limiter.qps);
        Assert.assertEquals(1, limiter.getRateLimiter().getRate(), 0.001);
    }

    @Test
    public void testBackpressureQpsLimiterApplyFactorVerySmall() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;
        double factor = 0.001;

        RpcRateLimiter.BackpressureQpsLimiter limiter =
                new RpcRateLimiter.BackpressureQpsLimiter(methodName, maxWaitRequestNum, qps, factor);

        // Even with very small factor, should be at least 1
        Assert.assertEquals(1, limiter.qps);
    }

    @Test
    public void testBackpressureQpsLimiterApplyFactorChanging() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;

        RpcRateLimiter.BackpressureQpsLimiter limiter =
                new RpcRateLimiter.BackpressureQpsLimiter(methodName, maxWaitRequestNum, qps, 0.8);

        // Initial factor 0.8
        Assert.assertEquals(80, limiter.qps);

        // Change to factor 0.3
        limiter.applyFactor(0.3);
        Assert.assertEquals(30, limiter.qps);

        // Change back to factor 1.0
        limiter.applyFactor(1.0);
        Assert.assertEquals(100, limiter.qps);
    }

    @Test
    public void testBackpressureQpsLimiterApplyFactorLargeQps() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 10000;
        double factor = 0.5;

        RpcRateLimiter.BackpressureQpsLimiter limiter =
                new RpcRateLimiter.BackpressureQpsLimiter(methodName, maxWaitRequestNum, qps, factor);

        // 10000 * 0.5 = 5000
        Assert.assertEquals(5000, limiter.qps);
    }

    // ==================== CostLimiter Tests ====================

    @Test
    public void testCostLimiterConstructor() {
        String methodName = "testMethod";
        int limit = 100;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        // Assert.assertEquals(methodName, limiter.methodName);
        Assert.assertEquals(limit, limiter.getLimit());
        Assert.assertEquals(0, limiter.getCurrentCost());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCostLimiterConstructorZeroLimit() {
        String methodName = "testMethod";
        int limit = 0;

        new RpcRateLimiter.CostLimiter(methodName, limit);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCostLimiterConstructorNegativeLimit() {
        String methodName = "testMethod";
        int limit = -1;

        new RpcRateLimiter.CostLimiter(methodName, limit);
    }

    @Test
    public void testCostLimiterSetLimit() {
        String methodName = "testMethod";
        int initialLimit = 100;
        int newLimit = 200;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, initialLimit);

        Assert.assertEquals(initialLimit, limiter.getLimit());

        limiter.setLimit(newLimit);

        Assert.assertEquals(newLimit, limiter.getLimit());
    }

    @Test
    public void testCostLimiterSetLimitSameValue() {
        String methodName = "testMethod";
        int limit = 100;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        // Set to same value - should not throw
        limiter.setLimit(limit);

        Assert.assertEquals(limit, limiter.getLimit());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCostLimiterSetLimitZero() {
        String methodName = "testMethod";
        int limit = 100;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        limiter.setLimit(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCostLimiterSetLimitNegative() {
        String methodName = "testMethod";
        int limit = 100;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        limiter.setLimit(-1);
    }

    @Test
    public void testCostLimiterAcquireSuccess() throws RpcRateLimitException, InterruptedException {
        String methodName = "testMethod";
        int limit = 100;
        int cost = 50;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        boolean acquired = limiter.acquire(cost);

        Assert.assertTrue(acquired);
        Assert.assertEquals(cost, limiter.getCurrentCost());
    }

    @Test
    public void testCostLimiterAcquireZeroCost() throws RpcRateLimitException, InterruptedException {
        String methodName = "testMethod";
        int limit = 100;
        int cost = 0;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        boolean acquired = limiter.acquire(cost);

        Assert.assertTrue(acquired);
        Assert.assertEquals(0, limiter.getCurrentCost());
    }

    @Test(expected = RpcRateLimitException.class)
    public void testCostLimiterAcquireExceedsLimit() throws RpcRateLimitException, InterruptedException {
        String methodName = "testMethod";
        int limit = 100;
        int cost = 150; // Exceeds limit

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        limiter.acquire(cost);
    }

    @Test(expected = RpcRateLimitException.class)
    public void testCostLimiterAcquireExactlyAtLimit() throws RpcRateLimitException, InterruptedException {
        // When cost == limit, it exceeds because currentCost + cost > limit
        String methodName = "testMethod";
        int limit = 100;
        int cost = 100;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        // This should fail because 0 + 100 > 100 is true
        limiter.acquire(cost);
    }

    @Test
    public void testCostLimiterAcquireBelowLimit() throws RpcRateLimitException, InterruptedException {
        String methodName = "testMethod";
        int limit = 100;
        int cost = 99; // Below limit (99 < 100)

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        boolean acquired = limiter.acquire(cost);

        Assert.assertTrue(acquired);
        Assert.assertEquals(cost, limiter.getCurrentCost());
    }

    @Test
    public void testCostLimiterRelease() throws RpcRateLimitException, InterruptedException {
        String methodName = "testMethod";
        int limit = 100;
        int cost = 50;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        limiter.acquire(cost);
        Assert.assertEquals(cost, limiter.getCurrentCost());

        limiter.release(cost);
        Assert.assertEquals(0, limiter.getCurrentCost());
    }

    @Test
    public void testCostLimiterReleaseMoreThanCurrent() throws RpcRateLimitException, InterruptedException {
        String methodName = "testMethod";
        int limit = 100;
        int cost = 30;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        limiter.acquire(cost);
        Assert.assertEquals(cost, limiter.getCurrentCost());

        // Release more than current cost - should clamp to 0
        limiter.release(50);
        Assert.assertEquals(0, limiter.getCurrentCost());
    }

    @Test
    public void testCostLimiterReleaseZero() throws RpcRateLimitException, InterruptedException {
        String methodName = "testMethod";
        int limit = 100;
        int cost = 50;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        limiter.acquire(cost);
        limiter.release(0);
        Assert.assertEquals(cost, limiter.getCurrentCost());
    }

    @Test
    public void testCostLimiterMultipleAcquireRelease() throws RpcRateLimitException, InterruptedException {
        String methodName = "testMethod";
        int limit = 100;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        // Acquire 30
        limiter.acquire(30);
        Assert.assertEquals(30, limiter.getCurrentCost());

        // Acquire another 30
        limiter.acquire(30);
        Assert.assertEquals(60, limiter.getCurrentCost());

        // Release 20
        limiter.release(20);
        Assert.assertEquals(40, limiter.getCurrentCost());

        // Acquire 50 more - 40 + 50 = 90 <= 100
        limiter.acquire(50);
        Assert.assertEquals(90, limiter.getCurrentCost());

        // Try to acquire 20 more - 90 + 20 = 110 > 100, should fail
        boolean acquired = limiter.acquire(20);
        Assert.assertFalse(acquired);
    }

    @Test
    public void testCostLimiterWaitForCapacity() throws InterruptedException, RpcRateLimitException {
        String methodName = "testMethod";
        int limit = 50;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 100; // Short timeout

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        // Acquire full capacity
        boolean acquired1 = limiter.acquire(50);
        Assert.assertTrue(acquired1);
        Assert.assertEquals(50, limiter.getCurrentCost());

        // Try to acquire more - should wait and fail due to timeout
        boolean acquired2 = limiter.acquire(10);
        Assert.assertFalse(acquired2);
    }

    @Test
    public void testCostLimiterReleaseSignalsWaiting() throws InterruptedException, RpcRateLimitException {
        String methodName = "testMethod";
        int limit = 50;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 2000;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        // Acquire full capacity
        limiter.acquire(50);
        Assert.assertEquals(50, limiter.getCurrentCost());

        // Start a thread that will try to acquire
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger result = new AtomicInteger(-1);

        Thread waiter = new Thread(() -> {
            try {
                boolean acquired = limiter.acquire(10);
                result.set(acquired ? 1 : 0);
            } catch (RpcRateLimitException e) {
                result.set(-1);
            } /*catch (InterruptedException e) {
                result.set(-2);
            }*/ finally {
                latch.countDown();
            }
        });
        waiter.start();

        // Give the waiter time to block
        Thread.sleep(100);

        // Release some capacity
        limiter.release(20);
        Assert.assertEquals(30, limiter.getCurrentCost());

        // Wait for the waiter to complete
        latch.await(2, TimeUnit.SECONDS);
        waiter.join(1000);

        // The waiter should have succeeded
        Assert.assertEquals(1, result.get());
    }

    @Test
    public void testCostLimiterGetLimit() {
        String methodName = "testMethod";
        int limit = 100;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        Assert.assertEquals(limit, limiter.getLimit());
    }

    @Test
    public void testCostLimiterGetCurrentCost() throws RpcRateLimitException, InterruptedException {
        String methodName = "testMethod";
        int limit = 100;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        Assert.assertEquals(0, limiter.getCurrentCost());

        limiter.acquire(30);
        Assert.assertEquals(30, limiter.getCurrentCost());

        limiter.acquire(20);
        Assert.assertEquals(50, limiter.getCurrentCost());

        limiter.release(10);
        Assert.assertEquals(40, limiter.getCurrentCost());
    }

    @Test
    public void testCostLimiterAcquireInterrupted() throws InterruptedException, RpcRateLimitException {
        String methodName = "testMethod";
        int limit = 100;
        int cost = 50;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 10000; // Long timeout

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        // Acquire full capacity
        limiter.acquire(100);
        Assert.assertEquals(100, limiter.getCurrentCost());

        // Create a thread that will try to acquire and get interrupted
        Thread waiter = new Thread(() -> {
            try {
                limiter.acquire(cost);
            } catch (RpcRateLimitException e) {
                // Expected when interrupted
            }
        });
        waiter.start();

        // Give the waiter time to block
        Thread.sleep(100);

        // Interrupt the waiter
        waiter.interrupt();
        waiter.join(1000);

        // The thread should have been interrupted
        Assert.assertFalse(waiter.isAlive());
    }

    // ==================== Integration Tests ====================

    @Test
    public void testQpsAndCostLimiterTogether() throws RpcRateLimitException, InterruptedException {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 1000;
        int costLimit = 100;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        RpcRateLimiter.QpsLimiter qpsLimiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, qps);
        RpcRateLimiter.CostLimiter costLimiter = new RpcRateLimiter.CostLimiter(methodName, costLimit);

        // Acquire QPS permit
        qpsLimiter.acquire();

        // Acquire cost permit
        boolean acquired = costLimiter.acquire(50);
        Assert.assertTrue(acquired);

        // Release cost
        costLimiter.release(50);
        Assert.assertEquals(0, costLimiter.getCurrentCost());
    }

    @Test
    public void testBackpressureAndCostLimiterTogether() throws RpcRateLimitException, InterruptedException {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;
        double factor = 0.5;
        int costLimit = 100;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        RpcRateLimiter.BackpressureQpsLimiter backpressureLimiter =
                new RpcRateLimiter.BackpressureQpsLimiter(methodName, maxWaitRequestNum, qps, factor);
        RpcRateLimiter.CostLimiter costLimiter = new RpcRateLimiter.CostLimiter(methodName, costLimit);

        // Backpressure effective QPS should be 50
        Assert.assertEquals(50, backpressureLimiter.qps);

        // Acquire backpressure permit
        backpressureLimiter.acquire();

        // Acquire cost permit
        boolean acquired = costLimiter.acquire(50);
        Assert.assertTrue(acquired);
    }

    @Test
    public void testConcurrentAccessToCostLimiter() throws InterruptedException {
        String methodName = "testMethod";
        int limit = 100;
        int threads = 10;
        int iterations = 20;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 5000;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < iterations; j++) {
                        try {
                            boolean acquired = limiter.acquire(10);
                            if (acquired) {
                                limiter.release(10);
                                successCount.incrementAndGet();
                            } else {
                                failCount.incrementAndGet();
                            }
                        } catch (RpcRateLimitException e) {
                            failCount.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        endLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        LOG.info("Concurrent test - Success: {}, Failed: {}", successCount.get(), failCount.get());
        // With concurrent access, some acquires should succeed and some should fail
        Assert.assertTrue(successCount.get() > 0);
    }

    @Test
    public void testMultipleBackpressureLimiters() {
        String methodName1 = "method1";
        String methodName2 = "method2";
        int maxWaitRequestNum = 10;
        int qps1 = 100;
        int qps2 = 200;

        RpcRateLimiter.BackpressureQpsLimiter limiter1 =
                new RpcRateLimiter.BackpressureQpsLimiter(methodName1, maxWaitRequestNum, qps1, 1.0);
        RpcRateLimiter.BackpressureQpsLimiter limiter2 =
                new RpcRateLimiter.BackpressureQpsLimiter(methodName2, maxWaitRequestNum, qps2, 0.5);

        Assert.assertEquals(100, limiter1.qps);
        Assert.assertEquals(100, limiter2.qps);

        // Apply different factors
        limiter1.applyFactor(0.5);
        limiter2.applyFactor(0.8);

        Assert.assertEquals(50, limiter1.qps);  // 100 * 0.5
        Assert.assertEquals(160, limiter2.qps); // 200 * 0.8
    }
}
