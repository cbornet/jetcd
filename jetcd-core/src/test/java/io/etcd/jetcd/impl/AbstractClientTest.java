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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.common.exception.EtcdException;
import io.etcd.jetcd.grpc.GrpcService;
import io.etcd.jetcd.support.Errors;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.grpc.client.InvalidStatusException;
import io.vertx.grpc.common.GrpcStatus;

import dev.failsafe.RetryPolicy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Unit tests for AbstractClient retry logic and error handling.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class AbstractClientTest {

    private Vertx vertx;
    private GrpcService grpcService;
    private TestClient testClient;
    private AtomicInteger authRefreshCount;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
        authRefreshCount = new AtomicInteger(0);
        grpcService = createGrpcService();
        testClient = new TestClient(grpcService, authRefreshCount);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (grpcService != null) {
            grpcService.close().get(5, TimeUnit.SECONDS);
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    // Helper methods

    private GrpcService createGrpcService() {
        ClientBuilder builder = Client.builder("http://localhost:2379")
            .retryMaxAttempts(3)
            .retryDelay(50)
            .retryMaxDelay(200)
            .retryChronoUnit(ChronoUnit.MILLIS)
            .vertx(vertx);

        return new GrpcService(builder);
    }

    private <T> Future<T> createSuccessfulFuture(T value) {
        return Future.succeededFuture(value);
    }

    private <T> Future<T> createFailedFuture(GrpcStatus status) {
        return Future.failedFuture(new InvalidStatusException(status, status));
    }

    private Supplier<Future<String>> createRetryingSupplier(int failTimes, String successValue, GrpcStatus failureStatus) {
        AtomicInteger attempts = new AtomicInteger(0);
        return () -> {
            int attempt = attempts.incrementAndGet();
            if (attempt <= failTimes) {
                return createFailedFuture(failureStatus);
            }
            return createSuccessfulFuture(successValue);
        };
    }

    // Test cases for Retry Policy Creation

    @Test
    void testRetryPolicyCreationWithMaxRetries() {
        Predicate<GrpcStatus> doRetry = Errors::isRetryableForSafeRedoOp;

        RetryPolicy<String> policy = testClient.testCreateRetryPolicy(doRetry);

        assertThat(policy).isNotNull();
        assertThat(policy.getConfig().getMaxRetries()).isEqualTo(3);
    }

    @Test
    void testRetryPolicyCreationWithBackoff() {
        Predicate<GrpcStatus> doRetry = Errors::isRetryableForSafeRedoOp;

        RetryPolicy<String> policy = testClient.testCreateRetryPolicy(doRetry);

        assertThat(policy).isNotNull();
        assertThat(policy.getConfig().getDelay()).isEqualTo(Duration.ofMillis(50));
        assertThat(policy.getConfig().getMaxDelay()).isEqualTo(Duration.ofMillis(200));
    }

    @Test
    void testRetryPolicyCreationWithMaxDuration() {
        ClientBuilder builder = Client.builder("http://localhost:2379")
            .retryMaxAttempts(3)
            .retryDelay(50)
            .retryMaxDelay(200)
            .retryMaxDuration(Duration.ofSeconds(5))
            .vertx(vertx);

        GrpcService serviceWithMaxDuration = new GrpcService(builder);
        TestClient clientWithMaxDuration = new TestClient(serviceWithMaxDuration, new AtomicInteger(0));

        RetryPolicy<String> policy = clientWithMaxDuration.testCreateRetryPolicy(Errors::isRetryableForSafeRedoOp);

        assertThat(policy).isNotNull();
        assertThat(policy.getConfig().getMaxDuration()).isEqualTo(Duration.ofSeconds(5));
    }

    // Test cases for Auth Token Refresh

    @Test
    void testAuthTokenRefreshOnUnauthenticated() throws Exception {
        authRefreshCount.set(0);

        Supplier<Future<String>> supplier = createRetryingSupplier(1, "success", GrpcStatus.UNAUTHENTICATED);
        CompletableFuture<String> result = testClient.testExecute(supplier, s -> s, Errors::isRetryableForSafeRedoOp);

        await().atMost(5, TimeUnit.SECONDS).until(() -> result.isDone());

        assertThat(result).isCompletedWithValue("success");
        assertThat(authRefreshCount.get()).isGreaterThan(0);
    }

    @Test
    void testAuthTokenRefreshOnAuthStoreExpired() throws Exception {
        authRefreshCount.set(0);

        Supplier<Future<String>> supplier = createRetryingSupplier(1, "success", GrpcStatus.INVALID_ARGUMENT);
        CompletableFuture<String> result = testClient.testExecute(supplier, s -> s, Errors::isRetryableForSafeRedoOp);

        await().atMost(5, TimeUnit.SECONDS).until(() -> result.isDone());

        assertThat(result).isCompletedWithValue("success");
        assertThat(authRefreshCount.get()).isGreaterThan(0);
    }

    @Test
    void testNonAuthErrorsDoNotTriggerRefresh() throws Exception {
        authRefreshCount.set(0);

        Supplier<Future<String>> supplier = createRetryingSupplier(1, "success", GrpcStatus.UNAVAILABLE);
        CompletableFuture<String> result = testClient.testExecute(supplier, s -> s, Errors::isRetryableForSafeRedoOp);

        await().atMost(5, TimeUnit.SECONDS).until(() -> result.isDone());

        assertThat(result).isCompletedWithValue("success");
        assertThat(authRefreshCount.get()).isEqualTo(0);
    }

    // Test cases for Retry Execution Logic

    @Test
    void testSuccessfulOperationWithoutRetry() throws Exception {
        Supplier<Future<String>> supplier = () -> createSuccessfulFuture("success");

        CompletableFuture<String> result = testClient.testExecute(supplier, s -> s, Errors::isRetryableForSafeRedoOp);

        assertThat(result).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo("success");
    }

    @Test
    void testOperationSucceedsAfterRetries() throws Exception {
        Supplier<Future<String>> supplier = createRetryingSupplier(2, "success", GrpcStatus.UNAVAILABLE);

        CompletableFuture<String> result = testClient.testExecute(supplier, s -> s, Errors::isRetryableForSafeRedoOp);

        assertThat(result).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo("success");
    }

    @Test
    void testOperationFailsAfterMaxRetries() {
        Supplier<Future<String>> supplier = () -> createFailedFuture(GrpcStatus.UNAVAILABLE);

        CompletableFuture<String> result = testClient.testExecute(supplier, s -> s, Errors::isRetryableForSafeRedoOp);

        assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
            .hasCauseInstanceOf(InvalidStatusException.class);
    }

    @Test
    void testRetryableVsNonRetryableErrors() {
        Supplier<Future<String>> retryableSupplier = () -> createFailedFuture(GrpcStatus.UNAVAILABLE);
        Supplier<Future<String>> nonRetryableSupplier = () -> createFailedFuture(GrpcStatus.NOT_FOUND);

        CompletableFuture<String> retryableResult = testClient.testExecute(
            retryableSupplier,
            s -> s,
            Errors::isRetryableForSafeRedoOp);

        CompletableFuture<String> nonRetryableResult = testClient.testExecute(
            nonRetryableSupplier,
            s -> s,
            Errors::isRetryableForSafeRedoOp);

        assertThatThrownBy(() -> retryableResult.get(5, TimeUnit.SECONDS))
            .hasCauseInstanceOf(InvalidStatusException.class);

        assertThatThrownBy(() -> nonRetryableResult.get(1, TimeUnit.SECONDS))
            .hasCauseInstanceOf(InvalidStatusException.class);
    }

    @Test
    void testSafeVsUnsafeRedoOperations() throws Exception {
        Supplier<Future<String>> supplier = createRetryingSupplier(1, "success", GrpcStatus.UNAVAILABLE);

        CompletableFuture<String> safeResult = testClient.testExecute(
            supplier,
            s -> s,
            Errors::isRetryableForSafeRedoOp);

        assertThat(safeResult).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo("success");

        Supplier<Future<String>> unsafeSupplier = () -> createFailedFuture(GrpcStatus.UNAVAILABLE);

        CompletableFuture<String> unsafeResult = testClient.testExecute(
            unsafeSupplier,
            s -> s,
            Errors::isRetryableForNoSafeRedoOp);

        assertThatThrownBy(() -> unsafeResult.get(1, TimeUnit.SECONDS))
            .hasCauseInstanceOf(InvalidStatusException.class);
    }

    // Test cases for Future to CompletableFuture Conversion

    @Test
    void testCompletableConvertsSucceededFuture() throws Exception {
        Future<String> future = createSuccessfulFuture("test-value");

        CompletableFuture<String> result = testClient.testCompletable(future, s -> s);

        assertThat(result).succeedsWithin(1, TimeUnit.SECONDS).isEqualTo("test-value");
    }

    @Test
    void testCompletableConvertsFailedFuture() {
        Future<String> future = createFailedFuture(GrpcStatus.INTERNAL);

        CompletableFuture<String> result = testClient.testCompletable(future, s -> s);

        assertThatThrownBy(() -> result.get(1, TimeUnit.SECONDS))
            .hasCauseInstanceOf(EtcdException.class);
    }

    @Test
    void testCompletableAppliesResultTransformation() throws Exception {
        Future<String> future = createSuccessfulFuture("test");

        CompletableFuture<Integer> result = testClient.testCompletable(future, String::length);

        assertThat(result).succeedsWithin(1, TimeUnit.SECONDS).isEqualTo(4);
    }

    @Test
    void testCompletableAppliesExceptionConversion() {
        Future<String> future = createFailedFuture(GrpcStatus.INTERNAL);

        CompletableFuture<String> result = testClient.testCompletable(future, s -> s);

        assertThatThrownBy(() -> result.get(1, TimeUnit.SECONDS))
            .hasCauseInstanceOf(EtcdException.class);
    }

    /**
     * Test client that exposes protected methods for testing and tracks auth refresh.
     */
    private static class TestClient extends AbstractClient {
        private final AtomicInteger authRefreshCount;

        TestClient(GrpcService grpcService, AtomicInteger authRefreshCount) {
            super(grpcService);
            this.authRefreshCount = authRefreshCount;
        }

        @Override
        protected <S> RetryPolicy<S> createRetryPolicy(Predicate<GrpcStatus> doRetry) {
            return super.createRetryPolicy(status -> {
                if (Errors.isInvalidTokenError(status) || Errors.isAuthStoreExpired(status)) {
                    authRefreshCount.incrementAndGet();
                }
                return doRetry.test(status);
            });
        }

        <S> RetryPolicy<S> testCreateRetryPolicy(Predicate<GrpcStatus> doRetry) {
            return super.createRetryPolicy(doRetry);
        }

        <S, T> CompletableFuture<T> testExecute(
            Supplier<Future<S>> supplier,
            java.util.function.Function<S, T> resultConvert,
            Predicate<GrpcStatus> doRetry) {

            return execute(supplier, resultConvert, doRetry);
        }

        <S, T> CompletableFuture<T> testCompletable(
            Future<S> sourceFuture,
            java.util.function.Function<S, T> resultConvert) {

            return completable(sourceFuture, resultConvert);
        }
    }
}

