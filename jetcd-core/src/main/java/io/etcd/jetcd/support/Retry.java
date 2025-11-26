/*
 * Copyright 2016-2021 The jetcd authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.etcd.jetcd.support;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe retry counter with configurable backoff strategies.
 */
public final class Retry {
    private final int maxAttempts;
    private final BackoffStrategy backoffStrategy;
    private final AtomicInteger attempts = new AtomicInteger(0);

    private Retry(Builder builder) {
        if (builder.maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (builder.backoffStrategy == null) {
            throw new IllegalArgumentException("backoffStrategy must not be null");
        }
        this.maxAttempts = builder.maxAttempts;
        this.backoffStrategy = builder.backoffStrategy;
    }

    /**
     * Create a new builder for configuring retry behavior.
     * @return new Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create a retry with exponential backoff strategy (convenience method).
     *
     * @param maxAttempts Maximum number of retry attempts
     * @param initialDelayMs Initial delay in milliseconds
     * @param maxDelayMs Maximum delay cap in milliseconds
     * @return Retry instance with exponential backoff
     */
    public static Retry exponentialBackoff(int maxAttempts, long initialDelayMs, long maxDelayMs) {
        return builder()
            .maxAttempts(maxAttempts)
            .exponentialBackoff(initialDelayMs, maxDelayMs)
            .build();
    }

    /**
     * Attempt a retry, incrementing the counter.
     * @return true if retry should proceed, false if max attempts exceeded
     */
    public boolean tryNextAttempt() {
        int current = attempts.incrementAndGet();
        return current <= maxAttempts;
    }

    /**
     * Calculate the delay for the current attempt using the configured backoff strategy.
     * @return delay in milliseconds
     */
    public long calculateDelay() {
        return backoffStrategy.calculateDelay(attempts.get());
    }

    /**
     * Get current attempt number (0-based).
     * @return current attempt count
     */
    public int getCurrentAttempt() {
        return attempts.get();
    }

    /**
     * Reset the retry counter to 0.
     */
    public void reset() {
        attempts.set(0);
    }

    /**
     * Check if max attempts have been reached.
     * @return true if no more retries allowed
     */
    public boolean isExhausted() {
        return attempts.get() >= maxAttempts;
    }

    /**
     * Strategy for calculating backoff delays.
     */
    public interface BackoffStrategy {
        /**
         * Calculate delay for the given attempt number.
         * @param attempt current attempt number (0-based)
         * @return delay in milliseconds
         */
        long calculateDelay(int attempt);
    }

    /**
     * Exponential backoff strategy: delay doubles with each attempt.
     * Formula: min(initialDelay * 2^(attempt-1), maxDelay)
     */
    public static class ExponentialBackoff implements BackoffStrategy {
        private final long initialDelayMs;
        private final long maxDelayMs;

        public ExponentialBackoff(long initialDelayMs, long maxDelayMs) {
            if (initialDelayMs < 0) {
                throw new IllegalArgumentException("initialDelayMs must be >= 0");
            }
            if (maxDelayMs < initialDelayMs) {
                throw new IllegalArgumentException("maxDelayMs must be >= initialDelayMs");
            }
            this.initialDelayMs = initialDelayMs;
            this.maxDelayMs = maxDelayMs;
        }

        @Override
        public long calculateDelay(int attempt) {
            if (attempt == 0) {
                return 0;
            }
            // Exponential backoff: initialDelay * 2^(attempt-1)
            long delay = initialDelayMs * (1L << (attempt - 1));
            return Math.min(delay, maxDelayMs);
        }
    }

    /**
     * Builder for constructing Retry instances with fluent API.
     */
    public static class Builder {
        private int maxAttempts = 3;
        private BackoffStrategy backoffStrategy;

        /**
         * Set maximum number of retry attempts.
         * @param maxAttempts must be >= 1
         * @return this builder
         */
        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        /**
         * Configure exponential backoff strategy.
         * @param initialDelayMs initial delay in milliseconds
         * @param maxDelayMs maximum delay cap in milliseconds
         * @return this builder
         */
        public Builder exponentialBackoff(long initialDelayMs, long maxDelayMs) {
            this.backoffStrategy = new ExponentialBackoff(initialDelayMs, maxDelayMs);
            return this;
        }

        /**
         * Configure custom backoff strategy.
         * @param strategy custom BackoffStrategy implementation
         * @return this builder
         */
        public Builder backoffStrategy(BackoffStrategy strategy) {
            this.backoffStrategy = strategy;
            return this;
        }

        /**
         * Build the Retry instance.
         * @return new Retry instance
         * @throws IllegalArgumentException if configuration is invalid
         */
        public Retry build() {
            return new Retry(this);
        }
    }
}
