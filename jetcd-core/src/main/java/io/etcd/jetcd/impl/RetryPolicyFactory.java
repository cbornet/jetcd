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

import dev.failsafe.RetryPolicy;
import dev.failsafe.RetryPolicyBuilder;
import io.vertx.grpc.client.InvalidStatusException;
import io.vertx.grpc.common.GrpcStatus;

import java.util.function.Predicate;

import static io.etcd.jetcd.support.Errors.isAuthStoreExpired;
import static io.etcd.jetcd.support.Errors.isInvalidTokenError;

/**
 * Factory for creating retry policies with authentication token refresh support.
 */
final class RetryPolicyFactory {
    private final GrpcService grpcService;

    RetryPolicyFactory(GrpcService grpcService) {
        this.grpcService = grpcService;
    }

    <S> RetryPolicy<S> createPolicy(Predicate<GrpcStatus> doRetry) {
        RetryPolicyBuilder<S> policy = RetryPolicy.<S> builder()
            .onFailure(e -> {
                // TODO: metrics
            })
            .onRetry(e -> {
                // TODO: metrics
            })
            .onRetriesExceeded(e -> {
                // TODO: metrics
            })
            .handleIf(throwable -> {
                GrpcStatus status = getGrpcStatus(throwable);
                if (isInvalidTokenError(status)) {
                    grpcService.auth().refreshToken();
                }
                if (isAuthStoreExpired(status)) {
                    grpcService.auth().refreshToken();
                }
                return doRetry.test(status);
            })
            .withMaxRetries(grpcService.builder().retryMaxAttempts())
            .withBackoff(
                grpcService.builder().retryDelay(),
                grpcService.builder().retryMaxDelay(),
                grpcService.builder().retryChronoUnit());

        if (grpcService.builder().retryMaxDuration() != null) {
            policy = policy.withMaxDuration(grpcService.builder().retryMaxDuration());
        }

        return policy.build();
    }

    private static GrpcStatus getGrpcStatus(Throwable throwable) {
        if (throwable instanceof InvalidStatusException invalidStatusException) {
            return invalidStatusException.actualStatus();
        }
        return GrpcStatus.UNKNOWN;
    }
}
