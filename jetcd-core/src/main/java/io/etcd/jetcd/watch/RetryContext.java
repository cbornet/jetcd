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

package io.etcd.jetcd.watch;

import java.time.Duration;

/**
 * Context information about a retry attempt.
 */
public record RetryContext(
    RetryType type,
    int attemptCount,
    int maxAttempts,
    Duration nextDelay,
    Throwable cause) {
    /**
     * The type of operation being retried
     */
    public enum RetryType {
        // Stream resume retry (connection/stream failure)
        RESUME,
        /// Progress request retry (write stream not ready)
        PROGRESS_REQUEST
    }

    /**
     * True if this is the last retry attempt
     */
    public boolean isLastAttempt() {
        return attemptCount >= maxAttempts;
    }
}
