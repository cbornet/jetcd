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

import io.etcd.jetcd.common.exception.EtcdExceptionFactory;
import io.etcd.jetcd.common.vertx.Failsafe;
import io.etcd.jetcd.support.Errors;
import io.vertx.core.Future;
import io.vertx.grpc.common.GrpcStatus;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Base class for service implementations providing common utilities.
 * Handles Future to CompletableFuture conversion and retry execution with Failsafe.
 */
abstract class AbstractService {
    private final GrpcService grpcService;
    private final RetryPolicyFactory retryPolicyFactory;

    protected AbstractService(GrpcService grpcService) {
        this.grpcService = grpcService;
        this.retryPolicyFactory = new RetryPolicyFactory(grpcService);
    }

    protected GrpcService grpc() {
        return this.grpcService;
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
            .with(retryPolicyFactory.createPolicy(doRetry))
            .with(Failsafe.vertxScheduler(grpcService.vertx()))
            .getStageAsync(() -> supplier.get().toCompletionStage())
            .thenApply(resultConvert);
    }
}
