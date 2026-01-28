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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MetaServiceRateLimiterTest {

    private boolean originalEnabled;
    private int originalDefaultQps;
    private int originalMaxWaiting;
    private long originalWaitTimeout;
    private String originalQpsConfig;

    @Before
    public void setUp() {
        originalEnabled = Config.meta_service_rpc_rate_limit_enabled;
        originalDefaultQps = Config.meta_service_rpc_rate_limit_default_qps;
        originalMaxWaiting = Config.meta_service_rpc_rate_limit_max_waiting;
        originalWaitTimeout = Config.meta_service_rpc_rate_limit_wait_timeout_ms;
        originalQpsConfig = Config.meta_service_rpc_rate_limit_qps_config;

        MetaServiceRateLimiter.resetInstance();
    }

    @After
    public void tearDown() {
        Config.meta_service_rpc_rate_limit_enabled = originalEnabled;
        Config.meta_service_rpc_rate_limit_default_qps = originalDefaultQps;
        Config.meta_service_rpc_rate_limit_max_waiting = originalMaxWaiting;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = originalWaitTimeout;
        Config.meta_service_rpc_rate_limit_qps_config = originalQpsConfig;

        MetaServiceRateLimiter.resetInstance();
    }

    @Test
    public void testDisabledByDefault() throws RpcRateLimitException {
        Config.meta_service_rpc_rate_limit_enabled = false;

        MetaServiceRateLimiter limiter = MetaServiceRateLimiter.getInstance();
        limiter.acquire("testMethod");
        limiter.release("testMethod");
    }

    @Test
    public void testBasicAcquireRelease() throws RpcRateLimitException {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps = 100;
        Config.meta_service_rpc_rate_limit_max_waiting = 10;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_config = "";

        MetaServiceRateLimiter limiter = MetaServiceRateLimiter.getInstance();
        limiter.acquire("testMethod");
        limiter.release("testMethod");
    }

    @Test
    public void testMethodSpecificQpsConfig() throws RpcRateLimitException {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps = 10;
        Config.meta_service_rpc_rate_limit_max_waiting = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_config = "getVersion:100;getTabletStats:50";

        MetaServiceRateLimiter limiter = MetaServiceRateLimiter.getInstance();

        limiter.acquire("getVersion");
        limiter.release("getVersion");

        limiter.acquire("getTabletStats");
        limiter.release("getTabletStats");

        limiter.acquire("otherMethod");
        limiter.release("otherMethod");
    }

    @Test
    public void testMaxWaitingRejects() {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps = 1;
        Config.meta_service_rpc_rate_limit_max_waiting = 2;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 10000;
        Config.meta_service_rpc_rate_limit_qps_config = "";

        MetaServiceRateLimiter limiter = MetaServiceRateLimiter.getInstance();

        ExecutorService executor = Executors.newFixedThreadPool(5);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch allStarted = new CountDownLatch(5);
        AtomicInteger rejectedCount = new AtomicInteger(0);
        AtomicInteger successCount = new AtomicInteger(0);
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            executor.submit(() -> {
                try {
                    allStarted.countDown();
                    startLatch.await();
                    limiter.acquire("testMethod");
                    successCount.incrementAndGet();
                    Thread.sleep(500);
                    limiter.release("testMethod");
                } catch (RpcRateLimitException e) {
                    if (e.getMessage().contains("too many waiting requests")) {
                        rejectedCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        try {
            allStarted.await(5, TimeUnit.SECONDS);
            startLatch.countDown();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Assert.assertTrue("Should have some rejected requests", rejectedCount.get() > 0);
    }

    @Test
    public void testConfigReload() throws RpcRateLimitException {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps = 100;
        Config.meta_service_rpc_rate_limit_max_waiting = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 1000;
        Config.meta_service_rpc_rate_limit_qps_config = "method1:10";

        MetaServiceRateLimiter limiter = MetaServiceRateLimiter.getInstance();
        limiter.acquire("method1");
        limiter.release("method1");

        Config.meta_service_rpc_rate_limit_qps_config = "method1:20;method2:30";
        limiter.reloadConfig();

        limiter.acquire("method1");
        limiter.release("method1");
        limiter.acquire("method2");
        limiter.release("method2");
    }

    @Test(expected = RpcRateLimitException.class)
    public void testWaitTimeout() throws RpcRateLimitException {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps = 1;
        Config.meta_service_rpc_rate_limit_max_waiting = 100;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 100;
        Config.meta_service_rpc_rate_limit_qps_config = "";

        MetaServiceRateLimiter limiter = MetaServiceRateLimiter.getInstance();

        for (int i = 0; i < 10; i++) {
            limiter.acquire("testMethod");
        }
    }
}
