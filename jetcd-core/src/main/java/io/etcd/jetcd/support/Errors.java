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

import io.vertx.grpc.client.InvalidStatusException;
import io.vertx.grpc.common.GrpcStatus;

public final class Errors {
    public static final String NO_LEADER_ERROR_MESSAGE = "etcdserver: no leader";
    public static final String INVALID_AUTH_TOKEN_ERROR_MESSAGE = "etcdserver: invalid auth token";
    public static final String ERROR_AUTH_STORE_OLD = "etcdserver: revision of auth store is old";

    private Errors() {
    }

    // isRetryable implementation for idempotent operations.
    public static boolean isRetryableForSafeRedoOp(GrpcStatus status) {
        return GrpcStatus.UNAVAILABLE.equals(status) || isAlwaysSafeToRetry(status);
    }

    // isRetryable implementation for non-idempotent operations
    public static boolean isRetryableForNoSafeRedoOp(GrpcStatus status) {
        return isAlwaysSafeToRetry(status);
    }

    public static boolean isAlwaysSafeToRetry(GrpcStatus status) {
        return isInvalidTokenError(status) || isAuthStoreExpired(status);
    }

    public static boolean isInvalidTokenError(Throwable e) {
        if (e instanceof InvalidStatusException invalidStatusException) {
            return isInvalidTokenError(invalidStatusException.actualStatus());
        }
        return false;
    }

    public static boolean isInvalidTokenError(GrpcStatus status) {
        // Note: Vert.x GrpcStatus doesn't have description/message
        // We'll need to check the exception message if needed
        return status == GrpcStatus.UNAUTHENTICATED || status == GrpcStatus.UNKNOWN;
    }

    public static boolean isAuthStoreExpired(Throwable e) {
        if (e instanceof InvalidStatusException invalidStatusException) {
            return isAuthStoreExpired(invalidStatusException.actualStatus());
        }
        return false;
    }

    public static boolean isAuthStoreExpired(GrpcStatus status) {
        return status == GrpcStatus.UNAUTHENTICATED || status == GrpcStatus.INVALID_ARGUMENT;
    }

    public static boolean isHaltError(final GrpcStatus status) {
        // Allow reconnection for transient errors:
        // - UNAVAILABLE: server temporarily unavailable
        // - INTERNAL: internal server errors
        // - UNKNOWN: unexpected stream closures (e.g., during cluster restarts)
        return status != GrpcStatus.UNAVAILABLE 
            && status != GrpcStatus.INTERNAL 
            && status != GrpcStatus.UNKNOWN;
    }

    public static boolean isNoLeaderError(final GrpcStatus status) {
        return status == GrpcStatus.UNAVAILABLE;
    }
}
