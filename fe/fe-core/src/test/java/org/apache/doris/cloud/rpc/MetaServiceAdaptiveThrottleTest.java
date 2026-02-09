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

import java.util.concurrent.atomic.AtomicInteger;

public class MetaServiceAdaptiveThrottleTest {

    private boolean originalEnabled;
    private double originalMinFactor;
    private double originalDecreaseMultiplier;
    private long originalCooldownMs;
    private long originalRecoveryIntervalMs;
    private double originalRecoveryStep;
    private int originalWindowSeconds;
    private int originalMinWindowRequests;
    private int originalBadTriggerCount;
    private double originalBadRateTrigger;

    @Before
    public void setUp() {
        originalEnabled = Config.meta_service_rpc_adaptive_throttle_enabled;
        originalMinFactor = Config.meta_service_rpc_adaptive_throttle_min_factor;
        originalDecreaseMultiplier = Config.meta_service_rpc_adaptive_throttle_decrease_multiplier;
        originalCooldownMs = Config.meta_service_rpc_adaptive_throttle_cooldown_ms;
        originalRecoveryIntervalMs = Config.meta_service_rpc_adaptive_throttle_recovery_interval_ms;
        originalRecoveryStep = Config.meta_service_rpc_adaptive_throttle_recovery_step;
        originalWindowSeconds = Config.meta_service_rpc_adaptive_throttle_window_seconds;
        originalMinWindowRequests = Config.meta_service_rpc_adaptive_throttle_min_window_requests;
        originalBadTriggerCount = Config.meta_service_rpc_adaptive_throttle_bad_trigger_count;
        originalBadRateTrigger = Config.meta_service_rpc_adaptive_throttle_bad_rate_trigger;

        Config.meta_service_rpc_adaptive_throttle_enabled = true;
        Config.meta_service_rpc_adaptive_throttle_min_factor = 0.1;
        Config.meta_service_rpc_adaptive_throttle_decrease_multiplier = 0.7;
        Config.meta_service_rpc_adaptive_throttle_cooldown_ms = 30000;
        Config.meta_service_rpc_adaptive_throttle_recovery_interval_ms = 5000;
        Config.meta_service_rpc_adaptive_throttle_recovery_step = 0.05;
        Config.meta_service_rpc_adaptive_throttle_window_seconds = 10;
        Config.meta_service_rpc_adaptive_throttle_min_window_requests = 5;
        Config.meta_service_rpc_adaptive_throttle_bad_trigger_count = 2;
        Config.meta_service_rpc_adaptive_throttle_bad_rate_trigger = 0.05;

        MetaServiceAdaptiveThrottle.resetInstance();
    }

    @After
    public void tearDown() {
        Config.meta_service_rpc_adaptive_throttle_enabled = originalEnabled;
        Config.meta_service_rpc_adaptive_throttle_min_factor = originalMinFactor;
        Config.meta_service_rpc_adaptive_throttle_decrease_multiplier = originalDecreaseMultiplier;
        Config.meta_service_rpc_adaptive_throttle_cooldown_ms = originalCooldownMs;
        Config.meta_service_rpc_adaptive_throttle_recovery_interval_ms = originalRecoveryIntervalMs;
        Config.meta_service_rpc_adaptive_throttle_recovery_step = originalRecoveryStep;
        Config.meta_service_rpc_adaptive_throttle_window_seconds = originalWindowSeconds;
        Config.meta_service_rpc_adaptive_throttle_min_window_requests = originalMinWindowRequests;
        Config.meta_service_rpc_adaptive_throttle_bad_trigger_count = originalBadTriggerCount;
        Config.meta_service_rpc_adaptive_throttle_bad_rate_trigger = originalBadRateTrigger;

        MetaServiceAdaptiveThrottle.resetInstance();
    }

    @Test
    public void testInitialState() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.NORMAL, throttle.getState());
        Assert.assertEquals(1.0, throttle.getFactor(), 0.001);
    }

    @Test
    public void testDisabledDoesNothing() {
        Config.meta_service_rpc_adaptive_throttle_enabled = false;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        for (int i = 0; i < 100; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.NORMAL, throttle.getState());
        Assert.assertEquals(1.0, throttle.getFactor(), 0.001);
    }

    @Test
    public void testSuccessKeepsNormal() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        for (int i = 0; i < 50; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.NORMAL, throttle.getState());
        Assert.assertEquals(1.0, throttle.getFactor(), 0.001);
    }

    @Test
    public void testTimeoutTriggersDecrease() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());
        Assert.assertTrue(throttle.getFactor() < 1.0);
    }

    @Test
    public void testBackpressureTriggersDecrease() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.BACKPRESSURE);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());
        Assert.assertTrue(throttle.getFactor() < 1.0);
    }

    @Test
    public void testFactorDoesNotGoBelowMin() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        for (int round = 0; round < 50; round++) {
            for (int i = 0; i < 5; i++) {
                throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
            }
            for (int i = 0; i < 5; i++) {
                throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
            }
        }

        Assert.assertTrue(throttle.getFactor() >= Config.meta_service_rpc_adaptive_throttle_min_factor);
    }

    @Test
    public void testFastDecreaseToCooldownTransition() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());

        for (int i = 0; i < 20; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.COOLDOWN, throttle.getState());
    }

    @Test
    public void testCooldownToSlowRecoveryTransition() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        triggerFastDecrease(throttle);
        transitionToCooldown(throttle);
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.COOLDOWN, throttle.getState());

        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.SLOW_RECOVERY, throttle.getState());
    }

    @Test
    public void testSlowRecoveryIncreasesFactorGradually() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        triggerFastDecrease(throttle);
        transitionToCooldown(throttle);

        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.SLOW_RECOVERY, throttle.getState());

        double factorBefore = throttle.getFactor();
        throttle.setLastRecoveryMs(System.currentTimeMillis() - 6000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);

        Assert.assertTrue(throttle.getFactor() > factorBefore);
    }

    @Test
    public void testSlowRecoveryBackToNormal() {
        Config.meta_service_rpc_adaptive_throttle_recovery_step = 0.5;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        triggerFastDecrease(throttle);
        transitionToCooldown(throttle);

        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.SLOW_RECOVERY, throttle.getState());

        for (int i = 0; i < 20; i++) {
            throttle.setLastRecoveryMs(System.currentTimeMillis() - 6000);
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
            if (throttle.getState() == MetaServiceAdaptiveThrottle.State.NORMAL) {
                break;
            }
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.NORMAL, throttle.getState());
        Assert.assertEquals(1.0, throttle.getFactor(), 0.001);
    }

    @Test
    public void testCooldownBadSignalGoesBackToFastDecrease() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        triggerFastDecrease(throttle);
        transitionToCooldown(throttle);
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.COOLDOWN, throttle.getState());

        double factorInCooldown = throttle.getFactor();
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());
        Assert.assertTrue(throttle.getFactor() <= factorInCooldown);
    }

    @Test
    public void testSlowRecoveryBadSignalGoesBackToFastDecrease() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        triggerFastDecrease(throttle);
        transitionToCooldown(throttle);
        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.SLOW_RECOVERY, throttle.getState());

        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.BACKPRESSURE);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());
    }

    @Test
    public void testFactorChangeListenerCalled() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();
        AtomicInteger callCount = new AtomicInteger(0);
        throttle.setFactorChangeListener(newFactor -> callCount.incrementAndGet());

        triggerFastDecrease(throttle);

        Assert.assertTrue(callCount.get() > 0);
    }

    @Test
    public void testBelowMinWindowRequestsDoesNotTrigger() {
        Config.meta_service_rpc_adaptive_throttle_min_window_requests = 100;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        for (int i = 0; i < 50; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.NORMAL, throttle.getState());
        Assert.assertEquals(1.0, throttle.getFactor(), 0.001);
    }

    @Test
    public void testWindowResets() {
        Config.meta_service_rpc_adaptive_throttle_window_seconds = 1;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        Assert.assertEquals(3, throttle.getWindowTotal());

        throttle.setWindowStartMs(System.currentTimeMillis() - 2000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        Assert.assertEquals(1, throttle.getWindowTotal());
    }

    private void triggerFastDecrease(MetaServiceAdaptiveThrottle throttle) {
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());
    }

    private void transitionToCooldown(MetaServiceAdaptiveThrottle throttle) {
        for (int i = 0; i < 20; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.COOLDOWN, throttle.getState());
    }
}
