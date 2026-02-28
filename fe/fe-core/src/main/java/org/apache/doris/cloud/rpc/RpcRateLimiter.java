package org.apache.doris.cloud.rpc;

import org.apache.doris.common.Config;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.metric.CloudMetrics;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RpcRateLimiter {
    private static final Logger LOG = LogManager.getLogger(RpcRateLimiter.class);
    /*private QpsRateLimiter qpsRateLimiter;
    private CostLimiter costLimiter;
    private QpsRateLimiter backpressureQpsRateLimiter;*/

    /*private interface MsRpcRateLimiter {
        boolean acquire(int cost) throws RpcRateLimitException;

        void release(int cost);

        void update(int maxWaitRequestNum, int qps, int costLimit);
    }*/

    protected static class QpsRateLimiter /*implements MsRpcRateLimiter*/ {
        protected final String methodName;
        private volatile int maxWaitRequestNum;
        private Semaphore waitingSemaphore;
        private RateLimiter rateLimiter;

        protected QpsRateLimiter(String methodName, int maxWaitRequestNum, int qps) {
            this.methodName = methodName;
            this.maxWaitRequestNum = maxWaitRequestNum;
            if (qps > 0) {
                this.waitingSemaphore = new Semaphore(maxWaitRequestNum);
                this.rateLimiter = RateLimiter.create(qps);
            }
            LOG.info("Create rate limiter for method: {}, maxWaitRequestNum: {}, qps: {}, cost: {}", methodName,
                    maxWaitRequestNum, qps);
        }

        void acquire() throws RpcRateLimitException {
            acquireQpsRateLimit(waitingSemaphore, rateLimiter);
        }

        /*boolean acquire(int cost) throws RpcRateLimitException {
            long startAt = System.nanoTime();
            boolean acquired = acquireCostLimit(costLimiter, cost);
            try {
                acquireQpsRateLimit(waitingSemaphore, rateLimiter);
                return acquired;
            } catch (RpcRateLimitException | RuntimeException e) {
                if (acquired) {
                    try {
                        release(cost);
                    } catch (Exception releaseEx) {
                        LOG.warn("Failed to release cost reservation for method {} after QPS limit failure",
                                methodName, releaseEx);
                    }
                }
                throw e;
            } finally {
                long durationNs = System.nanoTime() - startAt;
                SummaryProfile summaryProfile = SummaryProfile.getSummaryProfile(ConnectContext.get());
                if (summaryProfile != null) {
                    summaryProfile.addWaitMsRpcRateLimiterTime(durationNs);
                }
                if (MetricRepo.isInit && Config.isCloudMode()) {
                    CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED_LATENCY.getOrAdd(methodName)
                            .update(TimeUnit.NANOSECONDS.toMillis(durationNs));
                }
            }
        }*/

        /*private boolean acquireCostLimit(CostLimiter costLimiter, int cost) throws RpcRateLimitException {
            if (costLimiter == null || cost <= 0) {
                return false;
            }
            boolean acquired = false;
            try {
                acquired = costLimiter.acquire(cost, Config.meta_service_rpc_rate_limit_wait_timeout_ms,
                        TimeUnit.MILLISECONDS);
                if (!acquired) {
                    throw new RpcRateLimitException(
                            "Meta service rpc rate limit waiting timeout for cost limit for method: "
                                    + methodName + ", requestCost: " + cost + ", currentCost: "
                                    + costLimiter.currentCost + ", limit: " + costLimiter.limit);
                }
            } catch (InterruptedException e) {
                throw new RpcRateLimitException("Meta service rpc rate limit interrupted for cost limit for method: "
                        + methodName + ", requestCost: " + cost + ", currentCost: "
                        + costLimiter.currentCost + ", limit: " + costLimiter.limit, e);
            } finally {
                if (MetricRepo.isInit && Config.isCloudMode() && !acquired) {
                    CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED.getOrAdd(methodName).increase(1L);
                }
            }
            return acquired;
        }*/

        @VisibleForTesting
        void acquireQpsRateLimit(Semaphore waitingSemaphore, RateLimiter rateLimiter) throws RpcRateLimitException {
            if (rateLimiter == null || waitingSemaphore == null) {
                return;
            }
            // Try to acquire waiting semaphore first to avoid too many waiting requests
            if (!waitingSemaphore.tryAcquire()) {
                if (MetricRepo.isInit && Config.isCloudMode()) {
                    CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED.getOrAdd(methodName).increase(1L);
                }
                throw new RpcRateLimitException("Meta service rpc rate limit exceeded for method: " + methodName
                        + ", too many waiting requests (max=" + maxWaitRequestNum + ")");
            }
            // Try to acquire rate limiter permit with timeout
            try {
                long timeoutMs = Config.meta_service_rpc_rate_limit_wait_timeout_ms;
                boolean acquired = rateLimiter.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
                if (!acquired) {
                    if (MetricRepo.isInit && Config.isCloudMode()) {
                        CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED.getOrAdd(methodName).increase(1L);
                    }
                    throw new RpcRateLimitException(
                            "Meta service rpc rate limit timeout for method: " + methodName + ", rate: "
                                    + rateLimiter.getRate() + ", waited " + timeoutMs + " ms");
                }
            } catch (RpcRateLimitException e) {
                throw e;
            } catch (Exception e) {
                throw new RpcRateLimitException("Failed to acquire rate limit for method: " + methodName, e);
            } finally {
                waitingSemaphore.release();
            }
        }

        // Try to acquire without throwing - returns true if acquired, false otherwise
        /*boolean tryAcquire() {
            if (rateLimiter == null || waitingSemaphore == null) {
                return true; // No limiting
            }
            // Try to acquire waiting semaphore
            if (!waitingSemaphore.tryAcquire()) {
                return false;
            }
            // Try to acquire rate limiter permit (non-blocking)
            try {
                boolean acquired = rateLimiter.tryAcquire(0, TimeUnit.MILLISECONDS);
                if (!acquired) {
                    waitingSemaphore.release();
                    return false;
                }
                // Successfully acquired, release semaphore permit (it will be reacquired in actual acquire)
                waitingSemaphore.release();
                return true;
            } catch (Exception e) {
                waitingSemaphore.release();
                return false;
            }
        }*/

        /*void release(int cost) {
            if (costLimiter != null && cost > 0) {
                costLimiter.release(cost);
            }
        }*/

        /*void update(int maxWaitRequestNum, int qps, int costLimit) {
            updateQps(maxWaitRequestNum, qps);
            updateCostLimit(costLimit);
        }*/

        protected void updateQps(int maxWaitRequestNum, int qps) {
            if (qps <= 0) {
                rateLimiter = null;
                waitingSemaphore = null;
                return;
            }
            if (this.waitingSemaphore == null || maxWaitRequestNum != this.maxWaitRequestNum) {
                this.maxWaitRequestNum = maxWaitRequestNum;
                this.waitingSemaphore = new Semaphore(maxWaitRequestNum);
            }
            if (rateLimiter == null) {
                rateLimiter = RateLimiter.create(qps);
            } else if (qps != rateLimiter.getRate()) {
                rateLimiter.setRate(qps);
            }
            LOG.info("Update rate limiter for method: {}, maxWaitRequestNum: {}, qps: {}", methodName,
                    maxWaitRequestNum, qps);
        }

        /*private void updateCostLimit(int costLimit) {
            if (costLimit <= 0) {
                costLimiter = null;
                return;
            }
            if (costLimiter == null) {
                costLimiter = new CostLimiter(costLimit);
            } else {
                costLimiter.setLimit(costLimit);
            }
        }*/

        // only used for testing
        RateLimiter getRateLimiter() {
            return rateLimiter;
        }

        // only used for testing
        int getAllowWaiting() {
            return waitingSemaphore != null ? waitingSemaphore.availablePermits() : -1;
        }

        // only used for testing
        /*CostLimiter getCostLimiter() {
            return costLimiter;
        }*/

        // Get max wait request number - for subclass access
        protected int getMaxWaitRequestNum() {
            return maxWaitRequestNum;
        }
    }

    protected static class BackpressureQpsRateLimiter extends QpsRateLimiter {
        int baseQps;
        BackpressureQpsRateLimiter(String methodName, int maxWaitRequestNum, int qps, double factor) {
            super(methodName, maxWaitRequestNum, Math.max(1, (int) (qps * factor)));
            this.baseQps = qps;
        }

        void applyFactor(double factor) {
            // this.currentFactor = factor;
            int effectiveQps = Math.max(1, (int) (baseQps * factor));
            updateQps(Config.meta_service_rpc_rate_limit_max_waiting_request_num, effectiveQps);
            LOG.info("Applied factor {} to backpressure limiter for method {}, effective QPS: {}",
                    factor, methodName, effectiveQps);
        }
    }

    protected static class CostLimiter  {
        private String methodName;
        private volatile int limit;
        private int currentCost;
        private final Lock lock = new ReentrantLock(true);
        private final Condition condition = lock.newCondition();

        CostLimiter(String methodName, int limit) {
            this.methodName = methodName;
            if (limit < 0) {
                throw new IllegalArgumentException("limit must be >= 0");
            }
            this.limit = limit;
            this.currentCost = 0;
        }

        // modify limit to newLimit (thread safe)
        void setLimit(int newLimit) {
            if (newLimit < 0) {
                throw new IllegalArgumentException("newLimit must be >= 0");
            }
            lock.lock();
            try {
                this.limit = newLimit;
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }

        boolean acquire(int cost) throws RpcRateLimitException {
            if (cost > limit) {
                throw new RpcRateLimitException("Cost " + cost + " exceeds the limit " + limit);
            }
            long nanos = TimeUnit.MILLISECONDS.toNanos(Config.meta_service_rpc_rate_limit_wait_timeout_ms);
            try {
                lock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new RpcRateLimitException("Meta service rpc rate limit interrupted while acquiring lock for method: "
                        + methodName + ", requestCost: " + cost + ", currentCost: "
                        + currentCost + ", limit: " + limit, new InterruptedException());
            }
            try {
                while (currentCost + cost > limit) {
                    if (nanos <= 0) {
                        return false;
                    }
                    nanos = condition.awaitNanos(nanos);
                }
                currentCost += cost;
                return true;
            } catch (InterruptedException e) {
                throw new RpcRateLimitException(
                        "Meta service rpc rate limit interrupted while acquiring lock for method: "
                                + methodName + ", requestCost: " + cost + ", currentCost: "
                                + currentCost + ", limit: " + limit, new InterruptedException());
            } finally {
                lock.unlock();
            }
        }

        void release(int cost) {
            lock.lock();
            try {
                currentCost -= cost;
                if (currentCost < 0) {
                    currentCost = 0;
                }
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }

        int getLimit()  {
            return limit;
        }

        // only used for testing
        int getCurrentCost() {
            lock.lock();
            try {
                return currentCost;
            } finally {
                lock.unlock();
            }
        }
    }
}
