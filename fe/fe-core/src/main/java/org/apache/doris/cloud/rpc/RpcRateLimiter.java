package org.apache.doris.cloud.rpc;

import org.apache.doris.common.Config;
import org.apache.doris.metric.CloudMetrics;
import org.apache.doris.metric.MetricRepo;

import com.google.common.base.Preconditions;
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

    protected static class QpsLimiter {
        protected final String methodName;
        private volatile int maxWaitRequestNum;
        protected volatile int qps;
        private volatile Semaphore waitingSemaphore;
        private RateLimiter rateLimiter;

        protected QpsLimiter(String methodName, int maxWaitRequestNum, int qps) {
            Preconditions.checkArgument(qps > 0, "qps must be > 0");
            Preconditions.checkArgument(maxWaitRequestNum > 0, "maxWaitRequestNum must be > 0");
            this.methodName = methodName;
            this.maxWaitRequestNum = maxWaitRequestNum;
            this.waitingSemaphore = new Semaphore(maxWaitRequestNum);
            this.qps = qps;
            this.rateLimiter = RateLimiter.create(qps);
            LOG.info("Create rate limiter for method: {}, maxWaitRequestNum: {}, qps: {}", methodName,
                    maxWaitRequestNum, qps);
        }

        void acquire() throws RpcRateLimitException {
            acquireQpsRateLimit(waitingSemaphore, rateLimiter);
        }

        private void acquireQpsRateLimit(Semaphore waitingSemaphore, RateLimiter rateLimiter) throws RpcRateLimitException {
            // TODO
            if (waitingSemaphore == null) {
                return;
            }
            // Try to acquire waiting semaphore first to avoid too many waiting requests
            if (!waitingSemaphore.tryAcquire()) {
                /*if (MetricRepo.isInit && Config.isCloudMode()) {
                    CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED.getOrAdd(methodName).increase(1L);
                }*/
                throw new RpcRateLimitException("Meta service rpc rate limit exceeded for method: " + methodName
                        + ", too many waiting requests (max=" + maxWaitRequestNum + ")");
            }
            // Try to acquire rate limiter permit with timeout
            try {
                long timeoutMs = Config.meta_service_rpc_rate_limit_wait_timeout_ms;
                boolean acquired = rateLimiter.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
                if (!acquired) {
                    /*if (MetricRepo.isInit && Config.isCloudMode()) {
                        CloudMetrics.META_SERVICE_RPC_RATE_LIMIT_THROTTLED.getOrAdd(methodName).increase(1L);
                    }*/
                    throw new RpcRateLimitException(
                            "Meta service rpc rate limit timeout for method: " + methodName + ", rate: "
                                    + rateLimiter.getRate() + ", waited " + timeoutMs + " ms");
                }
            } /*catch (RpcRateLimitException e) {
                throw e;
            }*/ /*catch (Exception e) {
                throw new RpcRateLimitException("Failed to acquire rate limit for method: " + methodName, e);
            }*/ finally {
                waitingSemaphore.release();
            }
        }

        protected void updateQps(int maxWaitRequestNum, int qps) {
            Preconditions.checkArgument(qps > 0, "qps must be > 0");
            Preconditions.checkArgument(maxWaitRequestNum > 0, "maxWaitRequestNum must be > 0");
            /*if (qps <= 0) {
                rateLimiter = null;
                waitingSemaphore = null;
                return;
            }*/
            if (maxWaitRequestNum != this.maxWaitRequestNum) {
                this.maxWaitRequestNum = maxWaitRequestNum;
                this.waitingSemaphore = new Semaphore(maxWaitRequestNum);
            }
            if (qps != rateLimiter.getRate()) {
                this.qps = qps;
                this.rateLimiter.setRate(qps);
            }
            LOG.info("Update rate limiter for method: {}, maxWaitRequestNum: {}, qps: {}", methodName,
                    maxWaitRequestNum, qps);
        }

        // only used for testing
        RateLimiter getRateLimiter() {
            return rateLimiter;
        }

        // only used for testing
        int getAllowWaiting() {
            return waitingSemaphore != null ? waitingSemaphore.availablePermits() : -1;
        }

        // Get max wait request number - for subclass access
        protected int getMaxWaitRequestNum() {
            return maxWaitRequestNum;
        }
    }

    protected static class BackpressureQpsLimiter extends QpsLimiter {
        // int baseQps;
        BackpressureQpsLimiter(String methodName, int maxWaitRequestNum, int qps, double factor) {
            super(methodName, maxWaitRequestNum, qps);
            applyFactor(factor);
        }

        void applyFactor(double factor) {
            // this.currentFactor = factor;
            int effectiveQps = Math.max(1, (int) (qps * factor));
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
            Preconditions.checkArgument(limit > 0, "limit must be > 0");
            this.methodName = methodName;
            this.limit = limit;
            this.currentCost = 0;
        }

        // modify limit to newLimit (thread safe)
        void setLimit(int newLimit) {
            Preconditions.checkArgument(limit > 0, "limit must be > 0");
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
            try {
                lock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new RpcRateLimitException("Meta service rpc rate limit interrupted while acquiring lock for method: "
                        + methodName + ", requestCost: " + cost + ", currentCost: "
                        + currentCost + ", limit: " + limit, new InterruptedException());
            }
            try {
                long nanos = TimeUnit.MILLISECONDS.toNanos(Config.meta_service_rpc_rate_limit_wait_timeout_ms);
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
