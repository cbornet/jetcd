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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import dev.failsafe.RetryPolicy;
import dev.failsafe.RetryPolicyBuilder;
import io.etcd.jetcd.common.exception.EtcdExceptionFactory;
import io.etcd.jetcd.common.vertx.Failsafe;
import io.etcd.jetcd.grpc.GrpcService;
import io.etcd.jetcd.support.Errors;
import io.vertx.core.Future;
import io.vertx.grpc.client.InvalidStatusException;
import io.vertx.grpc.common.GrpcStatus;

import static io.etcd.jetcd.support.Errors.isAuthStoreExpired;
import static io.etcd.jetcd.support.Errors.isInvalidTokenError;

/**
 * Base class for service implementations providing common utilities.
 * Handles Future to CompletableFuture conversion and retry execution with Failsafe.
 */
abstract class AbstractClient {
    private final GrpcService grpcService;

    protected AbstractClient(GrpcService grpcService) {
        this.grpcService = grpcService;
    }

    protected GrpcService grpc() {
        return this.grpcService;
    }

    /**
     * Creates a retry policy with authentication token refresh support.
     *
     * @param  doRetry predicate to determine if a gRPC status should trigger a retry
     * @return         configured retry policy
     */
    protected <S> RetryPolicy<S> createRetryPolicy(Predicate<GrpcStatus> doRetry) {
        RetryPolicyBuilder<S> policy = RetryPolicy.<S>builder()
            .handleIf(throwable -> {
                GrpcStatus status = getGrpcStatus(throwable);
                if (isInvalidTokenError(status) || isAuthStoreExpired(status)) {
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

    /**
     * Converts Future of Type S to CompletableFuture of Type T.
     *
     * @param  sourceFuture  the Future to wrap
     * @param  resultConvert the result converter
     * @return               a {@link CompletableFuture} wrapping the given {@link Future}
     */
    protected <S, T> CompletableFuture<T> completable(Future<S> sourceFuture, Function<S, T> resultConvert) {
        return completable(sourceFuture, resultConvert, EtcdExceptionFactory::toEtcdException);
    }

    /**
     * Converts Future of Type S to CompletableFuture of Type T.
     *
     * @param  sourceFuture       the Future to wrap
     * @param  resultConvert      the result converter
     * @param  exceptionConverter the exception mapper
     * @return                    a {@link CompletableFuture} wrapping the given {@link Future}
     */
    protected <S, T> CompletableFuture<T> completable(
        Future<S> sourceFuture,
        Function<S, T> resultConvert,
        Function<Throwable, Throwable> exceptionConverter) {

        return completable(
            sourceFuture.compose(
                r -> Future.succeededFuture(resultConvert.apply(r)),
                e -> Future.failedFuture(exceptionConverter.apply(e))));
    }

    /**
     * Converts Future of Type S to CompletableFuture of Type T.
     *
     * @param  sourceFuture the Future to wrap
     * @return              a {@link CompletableFuture} wrapping the given {@link Future}
     */
    protected <S> CompletableFuture<S> completable(
        Future<S> sourceFuture) {
        return sourceFuture.toCompletionStage().toCompletableFuture();
    }

    /**
     * execute the task and retry it in case of failure.
     *
     * @param  supplier      a function that returns a new Future.
     * @param  resultConvert a function that converts Type S to Type T.
     * @param  <S>           Source type
     * @param  <T>           Converted Type.
     * @return               a CompletableFuture with type T.
     */
    protected <S, T> CompletableFuture<T> execute(
        Supplier<Future<S>> supplier,
        Function<S, T> resultConvert,
        boolean autoRetry) {

        return execute(
            supplier,
            resultConvert,
            autoRetry ? Errors::isRetryableForSafeRedoOp : Errors::isRetryableForNoSafeRedoOp);
    }

    /**
     * execute the task and retry it in case of failure.
     *
     * @param  supplier      a function that returns a new Future.
     * @param  resultConvert a function that converts Type S to Type T.
     * @param  doRetry       a predicate to determine if a failure has to be retried
     * @param  <S>           Source type
     * @param  <T>           Converted Type.
     * @return               a CompletableFuture with type T.
     */
    protected <S, T> CompletableFuture<T> execute(
        Supplier<Future<S>> supplier,
        Function<S, T> resultConvert,
        Predicate<GrpcStatus> doRetry) {

        return dev.failsafe.Failsafe
            .with(createRetryPolicy(doRetry))
            .with(Failsafe.vertxScheduler(grpcService.vertx()))
            .getStageAsync(() -> supplier.get().toCompletionStage())
            .thenApply(resultConvert);
    }
}
