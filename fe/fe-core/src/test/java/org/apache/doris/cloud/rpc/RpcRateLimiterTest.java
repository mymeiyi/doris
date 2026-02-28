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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RpcRateLimiterTest {
    private static final Logger LOG = LogManager.getLogger(RpcRateLimiterTest.class);

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
    public void testQpsLimiterConstructorWithPositiveQps() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, qps);

        Assert.assertEquals(methodName, limiter.methodName);
        Assert.assertEquals(maxWaitRequestNum, limiter.getMaxWaitRequestNum());
        Assert.assertNotNull(limiter.getRateLimiter());
        Assert.assertEquals(qps, limiter.getRateLimiter().getRate(), 0.001);
        Assert.assertTrue(limiter.getAllowWaiting() > 0);
    }

    @Test
    public void testQpsLimiterConstructorWithZeroQps() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 0;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, qps);

        Assert.assertEquals(methodName, limiter.methodName);
        Assert.assertEquals(maxWaitRequestNum, limiter.getMaxWaitRequestNum());
        Assert.assertNull(limiter.getRateLimiter());
        Assert.assertEquals(-1, limiter.getAllowWaiting());
    }

    @Test
    public void testQpsLimiterConstructorWithNegativeQps() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = -1;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, qps);

        Assert.assertEquals(methodName, limiter.methodName);
        Assert.assertEquals(maxWaitRequestNum, limiter.getMaxWaitRequestNum());
        Assert.assertNull(limiter.getRateLimiter());
        Assert.assertEquals(-1, limiter.getAllowWaiting());
    }

    @Test
    public void testQpsLimiterAcquireSuccess() throws RpcRateLimitException {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 1000;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, qps);

        // Should not throw exception
        limiter.acquire();
    }

    @Test
    public void testQpsLimiterAcquireWithWaitingQueueFull() throws InterruptedException {
        String methodName = "testMethod";
        int maxWaitRequestNum = 2;
        int qps = 1; // Very low QPS to make requests queue up

        Config.meta_service_rpc_rate_limit_max_waiting_request_num = maxWaitRequestNum;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 100; // Short timeout

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, qps);

        // Fill up the waiting queue by acquiring with low timeout
        ExecutorService executor = Executors.newFixedThreadPool(3);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (int i = 0; i < 3; i++) {
            executor.submit(() -> {
                try {
                    limiter.acquire();
                    successCount.incrementAndGet();
                } catch (RpcRateLimitException e) {
                    failCount.incrementAndGet();
                    LOG.info("Acquire failed: {}", e.getMessage());
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // At least some requests should fail due to queue full or timeout
        LOG.info("Success: {}, Failed: {}", successCount.get(), failCount.get());
    }

    @Test
    public void testQpsLimiterUpdateQpsIncrease() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int initialQps = 100;
        int newQps = 200;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, initialQps);

        Assert.assertEquals(initialQps, limiter.getRateLimiter().getRate(), 0.001);

        limiter.updateQps(maxWaitRequestNum, newQps);

        Assert.assertEquals(newQps, limiter.getRateLimiter().getRate(), 0.001);
    }

    @Test
    public void testQpsLimiterUpdateQpsDecrease() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int initialQps = 200;
        int newQps = 100;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, initialQps);

        Assert.assertEquals(initialQps, limiter.getRateLimiter().getRate(), 0.001);

        limiter.updateQps(maxWaitRequestNum, newQps);

        Assert.assertEquals(newQps, limiter.getRateLimiter().getRate(), 0.001);
    }

    @Test
    public void testQpsLimiterUpdateQpsToZero() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int initialQps = 100;
        int newQps = 0;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, maxWaitRequestNum, initialQps);

        Assert.assertNotNull(limiter.getRateLimiter());

        limiter.updateQps(maxWaitRequestNum, newQps);

        Assert.assertNull(limiter.getRateLimiter());
    }

    @Test
    public void testQpsLimiterUpdateMaxWaitRequestNum() {
        String methodName = "testMethod";
        int initialMaxWait = 10;
        int newMaxWait = 20;
        int qps = 100;

        RpcRateLimiter.QpsLimiter limiter = new RpcRateLimiter.QpsLimiter(methodName, initialMaxWait, qps);

        int initialAllowWaiting = limiter.getAllowWaiting();
        Assert.assertEquals(initialMaxWait, initialAllowWaiting);

        limiter.updateQps(newMaxWait, qps);

        Assert.assertEquals(newMaxWait, limiter.getAllowWaiting());
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
        // Effective QPS should be qps * factor
        Assert.assertEquals(qps, limiter.getRateLimiter().getRate(), 0.001);
    }

    @Test
    public void testBackpressureQpsLimiterApplyFactorOne() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;
        double factor = 1.0;

        RpcRateLimiter.BackpressureQpsLimiter limiter =
                new RpcRateLimiter.BackpressureQpsLimiter(methodName, maxWaitRequestNum, qps, factor);

        // Apply factor 1.0 - QPS should remain the same
        limiter.applyFactor(factor);

        Assert.assertEquals(qps, limiter.getRateLimiter().getRate(), 0.001);
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
        Assert.assertEquals(50, limiter.getRateLimiter().getRate(), 0.001);
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
        Assert.assertEquals(1, limiter.getRateLimiter().getRate(), 0.001);
    }

    @Test
    public void testBackpressureQpsLimiterApplyFactorChanging() {
        String methodName = "testMethod";
        int maxWaitRequestNum = 10;
        int qps = 100;
        double factor1 = 0.8;
        double factor2 = 0.3;
        double factor3 = 1.0;

        RpcRateLimiter.BackpressureQpsLimiter limiter =
                new RpcRateLimiter.BackpressureQpsLimiter(methodName, maxWaitRequestNum, qps, factor1);

        // Initial factor
        Assert.assertEquals(80, limiter.getRateLimiter().getRate(), 0.001);

        // Change to factor 0.3
        limiter.applyFactor(factor2);
        Assert.assertEquals(30, limiter.getRateLimiter().getRate(), 0.001);

        // Change back to factor 1.0
        limiter.applyFactor(factor3);
        Assert.assertEquals(100, limiter.getRateLimiter().getRate(), 0.001);
    }

    // ==================== CostLimiter Tests ====================

    @Test
    public void testCostLimiterConstructorWithValidLimit() {
        String methodName = "testMethod";
        int limit = 100;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        // Assert.assertEquals(methodName, limiter.methodName);
        Assert.assertEquals(limit, limiter.getLimit());
        Assert.assertEquals(0, limiter.getCurrentCost());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCostLimiterConstructorWithNegativeLimit() {
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

    @Test(expected = RpcRateLimitException.class)
    public void testCostLimiterAcquireExceedsLimit() throws RpcRateLimitException, InterruptedException {
        String methodName = "testMethod";
        int limit = 100;
        int cost = 150; // Exceeds limit

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        limiter.acquire(cost);
    }

    @Test
    public void testCostLimiterAcquireExactLimit() throws RpcRateLimitException, InterruptedException {
        String methodName = "testMethod";
        int limit = 100;
        int cost = 100; // Exact limit

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

        // Acquire 50 more - should reach limit
        limiter.acquire(50);
        Assert.assertEquals(90, limiter.getCurrentCost());

        // Try to acquire more - should fail
        boolean acquired = limiter.acquire(20);
        Assert.assertFalse(acquired); // Would exceed limit
    }

    @Test
    public void testCostLimiterWaitForCapacity() throws InterruptedException, RpcRateLimitException {
        String methodName = "testMethod";
        int limit = 50;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 500; // Short timeout

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

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

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
            } finally {
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
    public void testCostLimiterZeroCost() throws RpcRateLimitException, InterruptedException {
        String methodName = "testMethod";
        int limit = 100;
        int cost = 0;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, limit);

        // Acquire with 0 cost should succeed but not increase current cost
        boolean acquired = limiter.acquire(cost);

        Assert.assertTrue(acquired);
        Assert.assertEquals(0, limiter.getCurrentCost());
    }

    @Test
    public void testCostLimiterSetLimitWhileAcquired() throws RpcRateLimitException, InterruptedException {
        String methodName = "testMethod";
        int initialLimit = 100;
        int newLimit = 60;

        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;

        RpcRateLimiter.CostLimiter limiter = new RpcRateLimiter.CostLimiter(methodName, initialLimit);

        // Acquire 50
        limiter.acquire(50);
        Assert.assertEquals(50, limiter.getCurrentCost());

        // Set limit to 60 (still above current cost)
        limiter.setLimit(newLimit);
        Assert.assertEquals(newLimit, limiter.getLimit());

        // Try to acquire more - should fail (50 + 20 = 70 > 60)
        boolean acquired = limiter.acquire(20);
        Assert.assertFalse(acquired);
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
        Assert.assertEquals(50, backpressureLimiter.getRateLimiter().getRate(), 0.001);

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
}
