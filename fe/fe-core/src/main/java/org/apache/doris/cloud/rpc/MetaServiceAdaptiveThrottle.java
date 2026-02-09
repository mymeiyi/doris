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
import org.apache.doris.metric.CloudMetrics;
import org.apache.doris.metric.MetricRepo;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Adaptive throttle controller for meta-service RPCs.
 *
 * <p>Monitors RPC outcomes (success, timeout, backpressure) within a rolling time window
 * and adjusts a global factor (0.1–1.0) that scales the configured QPS limits in
 * {@link MetaServiceRateLimiter}. Uses a state machine:
 * NORMAL → FAST_DECREASE → COOLDOWN → SLOW_RECOVERY → NORMAL.
 */
public class MetaServiceAdaptiveThrottle {
    private static final Logger LOG = LogManager.getLogger(MetaServiceAdaptiveThrottle.class);

    public enum State {
        NORMAL,
        FAST_DECREASE,
        COOLDOWN,
        SLOW_RECOVERY
    }

    public enum Signal {
        SUCCESS,
        TIMEOUT,
        BACKPRESSURE
    }

    private volatile State state = State.NORMAL;
    private volatile double factor = 1.0;

    private final LongAdder windowTotal = new LongAdder();
    private final LongAdder windowBad = new LongAdder();
    private volatile long windowStartMs = System.currentTimeMillis();

    private volatile long cooldownStartMs = 0;
    private volatile long lastRecoveryMs = 0;

    private static volatile MetaServiceAdaptiveThrottle instance;

    private final AtomicReference<FactorChangeListener> listenerRef = new AtomicReference<>();

    public interface FactorChangeListener {
        void onFactorChanged(double newFactor);
    }

    private MetaServiceAdaptiveThrottle() {
    }

    public static MetaServiceAdaptiveThrottle getInstance() {
        if (instance == null) {
            synchronized (MetaServiceAdaptiveThrottle.class) {
                if (instance == null) {
                    instance = new MetaServiceAdaptiveThrottle();
                }
            }
        }
        return instance;
    }

    public void setFactorChangeListener(FactorChangeListener listener) {
        listenerRef.set(listener);
    }

    public void recordSignal(Signal signal) {
        if (!Config.meta_service_rpc_adaptive_throttle_enabled) {
            return;
        }

        long now = System.currentTimeMillis();
        maybeResetWindow(now);

        windowTotal.increment();
        if (signal == Signal.TIMEOUT || signal == Signal.BACKPRESSURE) {
            windowBad.increment();
            if (MetricRepo.isInit && Config.isCloudMode()) {
                if (signal == Signal.TIMEOUT) {
                    CloudMetrics.META_SERVICE_RPC_ADAPTIVE_THROTTLE_TIMEOUT_SIGNALS.increase(1L);
                } else {
                    CloudMetrics.META_SERVICE_RPC_ADAPTIVE_THROTTLE_BACKPRESSURE_SIGNALS.increase(1L);
                }
            }
        }

        switch (state) {
            case NORMAL:
                handleNormal(now);
                break;
            case FAST_DECREASE:
                handleFastDecrease(now);
                break;
            case COOLDOWN:
                handleCooldown(now, signal);
                break;
            case SLOW_RECOVERY:
                handleSlowRecovery(now, signal);
                break;
            default:
                break;
        }
    }

    private void handleNormal(long now) {
        if (isOverloaded()) {
            transitionTo(State.FAST_DECREASE, now);
            decreaseFactor();
        }
    }

    private void handleFastDecrease(long now) {
        if (isOverloaded()) {
            decreaseFactor();
        } else {
            transitionTo(State.COOLDOWN, now);
            cooldownStartMs = now;
        }
    }

    private void handleCooldown(long now, Signal signal) {
        if (signal == Signal.TIMEOUT || signal == Signal.BACKPRESSURE) {
            if (isOverloaded()) {
                transitionTo(State.FAST_DECREASE, now);
                decreaseFactor();
                return;
            }
        }

        long cooldownMs = Config.meta_service_rpc_adaptive_throttle_cooldown_ms;
        if (now - cooldownStartMs >= cooldownMs) {
            transitionTo(State.SLOW_RECOVERY, now);
            lastRecoveryMs = now;
        }
    }

    private void handleSlowRecovery(long now, Signal signal) {
        if (signal == Signal.TIMEOUT || signal == Signal.BACKPRESSURE) {
            if (isOverloaded()) {
                transitionTo(State.FAST_DECREASE, now);
                decreaseFactor();
                return;
            }
        }

        long recoveryIntervalMs = Config.meta_service_rpc_adaptive_throttle_recovery_interval_ms;
        if (now - lastRecoveryMs >= recoveryIntervalMs) {
            lastRecoveryMs = now;
            double step = Config.meta_service_rpc_adaptive_throttle_recovery_step;
            double newFactor = Math.min(1.0, factor + step);
            setFactor(newFactor);

            if (Double.compare(newFactor, 1.0) >= 0) {
                transitionTo(State.NORMAL, now);
            }
        }
    }

    private boolean isOverloaded() {
        long total = windowTotal.sum();
        long bad = windowBad.sum();
        int minRequests = Config.meta_service_rpc_adaptive_throttle_min_window_requests;
        int badTriggerCount = Config.meta_service_rpc_adaptive_throttle_bad_trigger_count;
        double badRateTrigger = Config.meta_service_rpc_adaptive_throttle_bad_rate_trigger;

        if (total < minRequests) {
            return false;
        }
        if (bad < badTriggerCount) {
            return false;
        }
        return (double) bad / total >= badRateTrigger;
    }

    private void decreaseFactor() {
        double multiplier = Config.meta_service_rpc_adaptive_throttle_decrease_multiplier;
        double minFactor = Config.meta_service_rpc_adaptive_throttle_min_factor;
        double newFactor = Math.max(minFactor, factor * multiplier);
        setFactor(newFactor);
    }

    private void setFactor(double newFactor) {
        double oldFactor = this.factor;
        this.factor = newFactor;

        if (Math.abs(newFactor - oldFactor) > 0.001) {
            LOG.info("Adaptive throttle factor changed: {} -> {} (state={})", oldFactor, newFactor, state);
            if (MetricRepo.isInit && Config.isCloudMode()) {
                CloudMetrics.META_SERVICE_RPC_ADAPTIVE_THROTTLE_FACTOR.setValue(newFactor);
            }
            FactorChangeListener listener = listenerRef.get();
            if (listener != null) {
                try {
                    listener.onFactorChanged(newFactor);
                } catch (Exception e) {
                    LOG.warn("Error notifying factor change listener", e);
                }
            }
        }
    }

    private void transitionTo(State newState, long now) {
        State oldState = this.state;
        this.state = newState;
        if (oldState != newState) {
            LOG.info("Adaptive throttle state transition: {} -> {} (factor={})", oldState, newState, factor);
            if (MetricRepo.isInit && Config.isCloudMode()) {
                CloudMetrics.META_SERVICE_RPC_ADAPTIVE_THROTTLE_STATE.setValue(newState.name());
            }
            resetWindow(now);
        }
    }

    private void maybeResetWindow(long now) {
        long windowMs = Config.meta_service_rpc_adaptive_throttle_window_seconds * 1000L;
        if (now - windowStartMs >= windowMs) {
            resetWindow(now);
        }
    }

    private void resetWindow(long now) {
        windowTotal.reset();
        windowBad.reset();
        windowStartMs = now;
    }

    public State getState() {
        return state;
    }

    public double getFactor() {
        return factor;
    }

    @VisibleForTesting
    public long getWindowTotal() {
        return windowTotal.sum();
    }

    @VisibleForTesting
    public long getWindowBad() {
        return windowBad.sum();
    }

    @VisibleForTesting
    public void reset() {
        state = State.NORMAL;
        factor = 1.0;
        windowTotal.reset();
        windowBad.reset();
        windowStartMs = System.currentTimeMillis();
        cooldownStartMs = 0;
        lastRecoveryMs = 0;
        listenerRef.set(null);
    }

    @VisibleForTesting
    public static void setInstanceForTest(MetaServiceAdaptiveThrottle testInstance) {
        instance = testInstance;
    }

    @VisibleForTesting
    public static void resetInstance() {
        instance = null;
    }

    @VisibleForTesting
    public void setWindowStartMs(long ms) {
        this.windowStartMs = ms;
    }

    @VisibleForTesting
    public void setCooldownStartMs(long ms) {
        this.cooldownStartMs = ms;
    }

    @VisibleForTesting
    public void setLastRecoveryMs(long ms) {
        this.lastRecoveryMs = ms;
    }
}
