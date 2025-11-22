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

package io.etcd.jetcd.impl;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.etcd.jetcd.support.Errors;
import io.vertx.grpc.common.GrpcStatus;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
class UtilTest {

    @Test
    public void testAuthStoreExpired() {
        // Test with INVALID_ARGUMENT status
        assertThat(Errors.isAuthStoreExpired(GrpcStatus.INVALID_ARGUMENT)).isTrue();
        // Test with UNAUTHENTICATED status
        assertThat(Errors.isAuthStoreExpired(GrpcStatus.UNAUTHENTICATED)).isTrue();
    }

    @Test
    public void testAuthErrorIsRetryable() {
        // UNAUTHENTICATED errors are retryable
        assertThat(Errors.isRetryableForNoSafeRedoOp(GrpcStatus.UNAUTHENTICATED)).isTrue();
        assertThat(Errors.isRetryableForSafeRedoOp(GrpcStatus.UNAUTHENTICATED)).isTrue();
    }

    @Test
    public void testUnavailableErrorIsRetryable() {
        // UNAVAILABLE errors are only retryable for safe redo operations
        assertThat(Errors.isRetryableForNoSafeRedoOp(GrpcStatus.UNAVAILABLE)).isFalse();
        assertThat(Errors.isRetryableForSafeRedoOp(GrpcStatus.UNAVAILABLE)).isTrue();
    }
}
