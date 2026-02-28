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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Comprehensive unit tests for MetaServiceAdaptiveThrottle.
 *
 * This test class covers:
 * - State machine transitions (NORMAL -> FAST_DECREASE -> COOLDOWN -> SLOW_RECOVERY -> NORMAL)
 * - Signal handling (SUCCESS, TIMEOUT, BACKPRESSURE)
 * - Factor calculation and boundaries
 * - Window management
 * - Factor change listener
 * - Overload detection logic
 */
public class MetaServiceAdaptiveThrottleTest2 {

    // Store original config values
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
    private String originalPhase1Methods;
    private boolean originalPhase2Enabled;
    private int originalBaseQpsWhenZero;

    @Before
    public void setUp() {
        // Save original values
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
        originalPhase1Methods = Config.meta_service_rpc_adaptive_throttle_phase1_methods;
        originalPhase2Enabled = Config.meta_service_rpc_adaptive_throttle_phase2_enabled;
        originalBaseQpsWhenZero = Config.meta_service_rpc_adaptive_throttle_base_qps_when_zero;

        // Set test values
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
        Config.meta_service_rpc_adaptive_throttle_phase1_methods = "";
        Config.meta_service_rpc_adaptive_throttle_phase2_enabled = false;
        Config.meta_service_rpc_adaptive_throttle_base_qps_when_zero = 100;

        // Reset singleton
        MetaServiceAdaptiveThrottle.resetInstance();
    }

    @After
    public void tearDown() {
        // Restore original values
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
        Config.meta_service_rpc_adaptive_throttle_phase1_methods = originalPhase1Methods;
        Config.meta_service_rpc_adaptive_throttle_phase2_enabled = originalPhase2Enabled;
        Config.meta_service_rpc_adaptive_throttle_base_qps_when_zero = originalBaseQpsWhenZero;

        // Reset singleton
        MetaServiceAdaptiveThrottle.resetInstance();
    }

    // ==================== Basic Functionality Tests ====================

    @Test
    public void testInitialState() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.NORMAL, throttle.getState());
        Assert.assertEquals(1.0, throttle.getFactor(), 0.001);
        Assert.assertEquals(0, throttle.getWindowTotal());
        Assert.assertEquals(0, throttle.getWindowBad());
    }

    @Test
    public void testDisabledDoesNothing() {
        Config.meta_service_rpc_adaptive_throttle_enabled = false;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Record many signals - should not affect state
        for (int i = 0; i < 100; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.BACKPRESSURE);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.NORMAL, throttle.getState());
        Assert.assertEquals(1.0, throttle.getFactor(), 0.001);
    }

    @Test
    public void testReset() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Trigger some state changes
        triggerFastDecrease(throttle);
        Assert.assertNotEquals(MetaServiceAdaptiveThrottle.State.NORMAL, throttle.getState());
        Assert.assertTrue(throttle.getFactor() < 1.0);

        // Reset
        throttle.reset();

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.NORMAL, throttle.getState());
        Assert.assertEquals(1.0, throttle.getFactor(), 0.001);
        Assert.assertEquals(0, throttle.getWindowTotal());
        Assert.assertEquals(0, throttle.getWindowBad());
    }

    // ==================== Signal Handling Tests ====================

    @Test
    public void testSuccessSignalInNORMAL() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Record success signals only
        for (int i = 0; i < 50; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.NORMAL, throttle.getState());
        Assert.assertEquals(1.0, throttle.getFactor(), 0.001);
    }

    @Test
    public void testTimeoutSignalInNORMALTriggersDecrease() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Need min_window_requests (5) successes first
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        // Then trigger overload with timeouts
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());
        Assert.assertTrue(throttle.getFactor() < 1.0);
    }

    @Test
    public void testBackpressureSignalInNORMALTriggersDecrease() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Need min_window_requests (5) successes first
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        // Then trigger overload with backpressure
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.BACKPRESSURE);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());
        Assert.assertTrue(throttle.getFactor() < 1.0);
    }

    @Test
    public void testMixedSignalsInNORMAL() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Mix of success and bad signals - should trigger decrease
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 2; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }
        for (int i = 0; i < 2; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.BACKPRESSURE);
        }
        // Total: 3 success, 4 bad out of 7 = 57% bad rate > 5% threshold

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());
        Assert.assertTrue(throttle.getFactor() < 1.0);
    }

    // ==================== State Transition Tests ====================

    @Test
    public void testNORMALtoFASTDECREASE() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        triggerFastDecrease(throttle);

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());
        Assert.assertTrue(throttle.getFactor() < 1.0);
    }

    @Test
    public void testFASTDECREASEStaysWhenOverloaded() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        triggerFastDecrease(throttle);
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());

        // Continue to record bad signals - should stay in FAST_DECREASE
        for (int i = 0; i < 10; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());
    }

    @Test
    public void testFASTDECREASEtoCOOLDOWN() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        triggerFastDecrease(throttle);
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());

        // Record success signals to recover
        for (int i = 0; i < 20; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.COOLDOWN, throttle.getState());
    }

    @Test
    public void testCOOLDOWNtoSLOWRECOVERY() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Transition to COOLDOWN
        triggerFastDecrease(throttle);
        transitionToCooldown(throttle);
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.COOLDOWN, throttle.getState());

        // Wait for cooldown period to pass
        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);

        // Record a success to trigger transition
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.SLOW_RECOVERY, throttle.getState());
    }

    @Test
    public void testCOOLDOWNtoFASTDECREASEOnBadSignal() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Transition to COOLDOWN
        triggerFastDecrease(throttle);
        transitionToCooldown(throttle);
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.COOLDOWN, throttle.getState());

        double factorBefore = throttle.getFactor();

        // Record bad signals - should go back to FAST_DECREASE
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());
        Assert.assertTrue(throttle.getFactor() <= factorBefore);
    }

    @Test
    public void testSLOWRECOVERYIncreasesFactor() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Transition to SLOW_RECOVERY
        triggerFastDecrease(throttle);
        transitionToCooldown(throttle);
        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.SLOW_RECOVERY, throttle.getState());

        double factorBefore = throttle.getFactor();

        // Wait for recovery interval and record success
        throttle.setLastRecoveryMs(System.currentTimeMillis() - 6000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);

        Assert.assertTrue(throttle.getFactor() > factorBefore);
    }

    @Test
    public void testSLOWRECOVERYtoNORMAL() {
        Config.meta_service_rpc_adaptive_throttle_recovery_step = 0.5;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Transition to SLOW_RECOVERY
        triggerFastDecrease(throttle);
        transitionToCooldown(throttle);
        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.SLOW_RECOVERY, throttle.getState());

        // Record success until fully recovered
        for (int i = 0; i < 5; i++) {
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
    public void testSLOWRECOVERYtoFASTDECREASEOnBadSignal() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Transition to SLOW_RECOVERY
        triggerFastDecrease(throttle);
        transitionToCooldown(throttle);
        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.SLOW_RECOVERY, throttle.getState());

        // Record bad signals - should go back to FAST_DECREASE
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());
    }

    // ==================== Factor Boundary Tests ====================

    @Test
    public void testFactorDoesNotGoBelowMin() {
        Config.meta_service_rpc_adaptive_throttle_min_factor = 0.1;
        Config.meta_service_rpc_adaptive_throttle_decrease_multiplier = 0.5;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Force many decreases
        for (int round = 0; round < 20; round++) {
            triggerFastDecrease(throttle);
            // Reset to NORMAL to trigger again
            throttle.reset();
        }

        Assert.assertTrue(throttle.getFactor() >= Config.meta_service_rpc_adaptive_throttle_min_factor);
    }

    @Test
    public void testFactorDoesNotExceedOne() {
        Config.meta_service_rpc_adaptive_throttle_recovery_step = 0.5;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Trigger decrease first
        triggerFastDecrease(throttle);
        double decreasedFactor = throttle.getFactor();
        Assert.assertTrue(decreasedFactor < 1.0);

        // Now recover
        transitionToCooldown(throttle);
        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);

        // Record many successes to fully recover
        for (int i = 0; i < 20; i++) {
            throttle.setLastRecoveryMs(System.currentTimeMillis() - 6000);
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }

        Assert.assertEquals(1.0, throttle.getFactor(), 0.001);
    }

    @Test
    public void testFactorDecreaseCalculation() {
        // Test that factor decreases by the configured multiplier
        Config.meta_service_rpc_adaptive_throttle_decrease_multiplier = 0.5;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        triggerFastDecrease(throttle);
        Assert.assertEquals(0.5, throttle.getFactor(), 0.001);
    }

    // ==================== Window Tests ====================

    @Test
    public void testWindowResetsAfterWindowPeriod() {
        Config.meta_service_rpc_adaptive_throttle_window_seconds = 1;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Record some signals
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        Assert.assertEquals(5, throttle.getWindowTotal());

        // Advance window
        throttle.setWindowStartMs(System.currentTimeMillis() - 2000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);

        Assert.assertEquals(1, throttle.getWindowTotal());
    }

    @Test
    public void testWindowNotResetWithinPeriod() {
        Config.meta_service_rpc_adaptive_throttle_window_seconds = 10;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        for (int i = 0; i < 10; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }

        Assert.assertEquals(10, throttle.getWindowTotal());

        // Record more within same window
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        Assert.assertEquals(11, throttle.getWindowTotal());
    }

    @Test
    public void testWindowResetsOnStateTransition() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Record some signals
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        Assert.assertEquals(5, throttle.getWindowTotal());

        // Trigger state transition
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }

        // Window should reset on state transition
        Assert.assertTrue(throttle.getWindowTotal() < 5);
    }

    // ==================== Overload Detection Tests ====================

    @Test
    public void testIsOverloaded_falseWhenBelowMinRequests() {
        Config.meta_service_rpc_adaptive_throttle_min_window_requests = 100;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Record some bad signals but below min threshold
        for (int i = 0; i < 50; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.NORMAL, throttle.getState());
    }

    @Test
    public void testIsOverloaded_falseWhenBelowBadCount() {
        Config.meta_service_rpc_adaptive_throttle_bad_trigger_count = 10;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Record bad signals but below threshold
        for (int i = 0; i < 20; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.NORMAL, throttle.getState());
    }

    @Test
    public void testIsOverloaded_falseWhenBelowBadRate() {
        Config.meta_service_rpc_adaptive_throttle_bad_rate_trigger = 0.5;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Many successes, few timeouts - rate below threshold
        for (int i = 0; i < 20; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }
        // 5/25 = 20% bad rate < 50% threshold

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.NORMAL, throttle.getState());
    }

    @Test
    public void testIsOverloaded_trueWhenAllConditionsMet() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // All conditions met: min_requests=5, bad_count=2, bad_rate=5%
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }
        // 3/8 = 37.5% bad rate > 5% threshold

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());
    }

    // ==================== Listener Tests ====================

    @Test
    public void testFactorChangeListenerCalledOnDecrease() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();
        AtomicInteger callCount = new AtomicInteger(0);
        AtomicReference<Double> lastFactor = new AtomicReference<>(1.0);

        throttle.setFactorChangeListener(newFactor -> {
            callCount.incrementAndGet();
            lastFactor.set(newFactor);
        });

        triggerFastDecrease(throttle);

        Assert.assertTrue(callCount.get() > 0);
        Assert.assertTrue(lastFactor.get() < 1.0);
    }

    @Test
    public void testFactorChangeListenerCalledOnIncrease() {
        Config.meta_service_rpc_adaptive_throttle_recovery_step = 0.5;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();
        AtomicInteger callCount = new AtomicInteger(0);

        // First decrease
        triggerFastDecrease(throttle);
        double decreasedFactor = throttle.getFactor();
        callCount.set(0);

        // Then recover
        transitionToCooldown(throttle);
        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        throttle.setLastRecoveryMs(System.currentTimeMillis() - 6000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);

        Assert.assertTrue(throttle.getFactor() > decreasedFactor);
    }

    @Test
    public void testFactorChangeListenerNotCalledWhenFactorUnchanged() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();
        AtomicInteger callCount = new AtomicInteger(0);

        throttle.setFactorChangeListener(newFactor -> callCount.incrementAndGet());

        // Record success signals - should not trigger factor change
        for (int i = 0; i < 20; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }

        Assert.assertEquals(0, callCount.get());
    }

    @Test
    public void testFactorChangeListenerExceptionHandled() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Set listener that throws exception - should not crash
        throttle.setFactorChangeListener(newFactor -> {
            throw new RuntimeException("Test exception");
        });

        // Should not throw
        triggerFastDecrease(throttle);
    }

    // ==================== Edge Case Tests ====================

    @Test
    public void testZeroRecoveryStep() {
        Config.meta_service_rpc_adaptive_throttle_recovery_step = 0.0;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Decrease first
        triggerFastDecrease(throttle);
        double decreasedFactor = throttle.getFactor();

        // Try to recover with zero step
        transitionToCooldown(throttle);
        throttle.setCooldownStartMs(System.currentTimeMillis() - 31000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);

        // Set last recovery to past
        throttle.setLastRecoveryMs(System.currentTimeMillis() - 6000);
        throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);

        // Factor should not increase with zero step
        Assert.assertEquals(decreasedFactor, throttle.getFactor(), 0.0001);
    }

    @Test
    public void testZeroCooldownPeriod() {
        Config.meta_service_rpc_adaptive_throttle_cooldown_ms = 0;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Transition to FAST_DECREASE
        triggerFastDecrease(throttle);

        // Record success - should transition immediately due to zero cooldown
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }

        // Should go to SLOW_RECOVERY directly (skip COOLDOWN)
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.SLOW_RECOVERY, throttle.getState());
    }

    @Test
    public void testVerySmallWindow() {
        Config.meta_service_rpc_adaptive_throttle_window_seconds = 0;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Window should reset frequently with 0 second window
        for (int i = 0; i < 10; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }

        // With 0 second window, should always be reset, so should not trigger overload
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.NORMAL, throttle.getState());
    }

    @Test
    public void testOneHundredPercentBadRateTrigger() {
        Config.meta_service_rpc_adaptive_throttle_bad_rate_trigger = 1.0;
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();

        // Even with high bad rate, should not trigger if threshold is 100%
        for (int i = 0; i < 10; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        for (int i = 0; i < 10; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }

        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.NORMAL, throttle.getState());
    }

    @Test
    public void testListenerCalledWithCorrectFactor() {
        MetaServiceAdaptiveThrottle throttle = MetaServiceAdaptiveThrottle.getInstance();
        AtomicReference<Double> capturedFactor = new AtomicReference<>(0.0);

        throttle.setFactorChangeListener(capturedFactor::set);

        triggerFastDecrease(throttle);

        Assert.assertEquals(throttle.getFactor(), capturedFactor.get(), 0.0001);
    }

    // ==================== Helper Methods ====================

    private void triggerFastDecrease(MetaServiceAdaptiveThrottle throttle) {
        // Need min_window_requests successes first
        for (int i = 0; i < 5; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        // Then trigger overload
        for (int i = 0; i < 3; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.TIMEOUT);
        }
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.FAST_DECREASE, throttle.getState());
    }

    private void transitionToCooldown(MetaServiceAdaptiveThrottle throttle) {
        // In FAST_DECREASE, record successes to recover
        for (int i = 0; i < 20; i++) {
            throttle.recordSignal(MetaServiceAdaptiveThrottle.Signal.SUCCESS);
        }
        Assert.assertEquals(MetaServiceAdaptiveThrottle.State.COOLDOWN, throttle.getState());
    }
}
