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
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.api.WatchCancelRequest;
import io.etcd.jetcd.api.WatchCreateRequest;
import io.etcd.jetcd.api.WatchProgressRequest;
import io.etcd.jetcd.api.WatchRequest;
import io.etcd.jetcd.common.exception.EtcdException;
import io.etcd.jetcd.common.exception.Exceptions;
import io.etcd.jetcd.common.vertx.Failsafe;
import io.etcd.jetcd.options.OptionsUtil;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.support.Util;
import io.etcd.jetcd.watch.RetryContext;
import io.etcd.jetcd.watch.WatchResponse;
import io.vertx.core.Vertx;
import io.vertx.grpc.common.GrpcStatus;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.newClosedWatchClientException;
import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.toEtcdException;

/**
 * Watch implementation where each watcher manages its own dedicated gRPC stream.
 */
final class WatchImpl extends AbstractService implements Watch {
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(15);

    private final AtomicBoolean closed;
    private final List<Watcher> watchers;
    private final ByteSequence namespace;

    WatchImpl(GrpcService grpcService) {
        super(grpcService);
        this.closed = new AtomicBoolean();
        this.watchers = new CopyOnWriteArrayList<>();
        this.namespace = grpcService.getNamespace();
    }

    @Override
    public Watcher watch(ByteSequence key, WatchOption option, Listener listener) {
        if (closed.get()) {
            throw newClosedWatchClientException();
        }

        WatcherImpl watcher = new WatcherImpl(
            key,
            namespace,
            option,
            listener,
            grpc(),
            watchers::remove);

        watchers.add(watcher);
        return watcher;
    }

    @Override
    public CompletableFuture<Watcher> watchAsync(ByteSequence key, WatchOption option, Listener listener) {
        if (closed.get()) {
            throw newClosedWatchClientException();
        }

        WatcherImpl watcher = new WatcherImpl(
            key,
            namespace,
            option,
            listener,
            grpc(),
            watchers::remove);

        watchers.add(watcher);
        return watcher.getReadyFuture().thenApply(v -> watcher);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        Exceptions.quietly(() -> {
            try {
                CompletableFuture<?> f = CompletableFuture.allOf(
                    watchers.stream()
                        .map(Watcher::closeAsync)
                        .toArray(CompletableFuture[]::new));

                f.get(CLOSE_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void requestProgress() {
        if (closed.get()) {
            return;
        }

        for (Watcher watcher : watchers) {
            Exceptions.quietly(watcher::requestProgress);
        }
    }

    /**
     * Individual watcher that owns its dedicated gRPC stream.
     * Uses WatchStateMachine for lifecycle management and WatchStream for gRPC stream handling.
     */
    private static final class WatcherImpl implements Watch.Watcher, WatchStream.Handler, WatchStateMachine.Handler {
        private static final int MAX_RECONNECT_ATTEMPTS = 10;
        private static final Duration INITIAL_RECONNECT_DELAY = Duration.ofMillis(500);
        private static final Duration MAX_RECONNECT_DELAY = Duration.ofSeconds(30);

        private final ByteSequence key;
        private final ByteSequence namespace;
        private final WatchOption option;
        private final Watch.Listener listener;
        private final GrpcService grpcService;
        private final Vertx vertx;
        private final java.util.function.Consumer<WatcherImpl> onClose;
        private final WatchResponseProcessor responseProcessor;
        private final WatchStateMachine stateMachine;
        private final CompletableFuture<Void> readyFuture = new CompletableFuture<>();
        private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        // Event loop only fields
        private WatchStream watchStream;
        private CompletableFuture<Void> reconnectFuture;
        private boolean pendingProgressRequest;
        private long revision;

        WatcherImpl(
            ByteSequence key,
            ByteSequence namespace,
            WatchOption option,
            Watch.Listener listener,
            GrpcService grpcService,
            java.util.function.Consumer<WatcherImpl> onClose) {

            this.key = key;
            this.namespace = namespace;
            this.option = option;
            this.listener = listener;
            this.revision = option.getRevision();
            this.grpcService = grpcService;
            this.vertx = grpcService.vertx();
            this.onClose = onClose;
            this.responseProcessor = new WatchResponseProcessor(
                option.isCreatedNotify(),
                option.isProgressNotify());
            this.stateMachine = new WatchStateMachine(vertx, this);

            // Start the state machine
            stateMachine.start();
        }

        @Override
        public boolean isClosed() {
            return stateMachine.isClosed();
        }

        CompletableFuture<Void> getReadyFuture() {
            return readyFuture;
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            stateMachine.close();
            return closeFuture;
        }

        @Override
        public void requestProgress() {
            if (stateMachine.isClosed()) {
                return;
            }

            vertx.runOnContext(v -> {
                if (!stateMachine.isWatching()) {
                    pendingProgressRequest = true;
                    return;
                }

                WatchStream stream = watchStream;
                if (stream == null || !stream.isWriteStreamReady()) {
                    pendingProgressRequest = true;
                    return;
                }

                WatchProgressRequest progress = WatchProgressRequest.newBuilder().build();
                stream.send(WatchRequest.newBuilder().setProgressRequest(progress).build());
                pendingProgressRequest = false;
            });
        }

        // WatchStateMachine.Handler implementation

        @Override
        public void onConnect() {
            watchStream = new WatchStream(grpcService);
            watchStream.connect(this).onComplete(ar -> {
                if (ar.failed()) {
                    stateMachine.streamError(ar.cause());
                }
            });
        }

        @Override
        public void onSubscribe() {
            WatchStream stream = watchStream;
            if (stream != null) {
                stream.send(WatchRequest.newBuilder()
                    .setCreateRequest(buildCreateRequest())
                    .build());
            }
        }

        @Override
        public void onReady() {
            readyFuture.complete(null);

            // Send pending progress request if one was queued
            if (pendingProgressRequest) {
                pendingProgressRequest = false;
                WatchStream stream = watchStream;
                if (stream != null && stream.isWriteStreamReady()) {
                    WatchProgressRequest progress = WatchProgressRequest.newBuilder().build();
                    stream.send(WatchRequest.newBuilder().setProgressRequest(progress).build());
                }
            }
        }

        @Override
        public void onReconnect() {
            if (reconnectFuture != null && !reconnectFuture.isDone()) {
                return;
            }

            reconnectFuture = Failsafe.runAsync(vertx, () -> {
                disconnect();
            }, buildRetryPolicy()).whenComplete((result, error) -> {
                reconnectFuture = null;
                if (error == null) {
                    stateMachine.reconnectSucceeded();
                } else {
                    stateMachine.reconnectFailed(error);
                }
            });
        }

        @Override
        public void onClose() {
            // Cancel pending reconnect
            if (reconnectFuture != null && !reconnectFuture.isDone()) {
                reconnectFuture.cancel(true);
                reconnectFuture = null;
            }

            sendCancelRequest();
            disconnect();

            // Notify listener
            Exceptions.quietly(listener::onCompleted);
            Exceptions.quietly(() -> onClose.accept(this));

            closeFuture.complete(null);
        }

        @Override
        public void onStateError(Throwable error) {
            EtcdException etcdException = toEtcdException(error);
            Exceptions.quietly(() -> listener.onError(etcdException));
            readyFuture.completeExceptionally(error);
        }

        // WatchStream.Handler implementation

        @Override
        public void onMessage(io.etcd.jetcd.api.WatchResponse response) {
            if (stateMachine.isClosed()) {
                return;
            }

            WatchResponseProcessor.Result result = responseProcessor.process(response);

            switch (result) {
                case WatchResponseProcessor.Result.AuthError() -> {
                    grpcService.auth().refreshToken();
                    stateMachine.streamError(toEtcdException(GrpcStatus.CANCELLED));
                }
                case WatchResponseProcessor.Result.Created(long rev, boolean shouldNotify) -> {
                    updateRevision(rev);
                    stateMachine.watchCreated();
                    if (shouldNotify) {
                        notifyListener(response, false);
                    }
                }
                case WatchResponseProcessor.Result.Canceled(Throwable error) -> {
                    stateMachine.watchCanceled(error);
                }
                case WatchResponseProcessor.Result.Progress(long rev, boolean withNamespace) -> {
                    notifyListener(response, withNamespace);
                    updateRevision(rev);
                }
                case WatchResponseProcessor.Result.Events(long newRevision) -> {
                    notifyListener(response, true);
                    revision = newRevision;
                }
                case WatchResponseProcessor.Result.Ignored() -> {
                    // No action needed
                }
            }
        }

        @Override
        public void onEnd() {
            stateMachine.streamEnded();
        }

        @Override
        public void onError(Throwable error) {
            stateMachine.streamError(error);
        }

        @Override
        public void onWriteStreamReady(WatchStream stream) {
            stateMachine.streamReady();
        }

        // Private helpers

        private void disconnect() {
            pendingProgressRequest = false;

            WatchStream ws = watchStream;
            watchStream = null;

            if (ws != null) {
                ws.disconnect();
            }
        }

        private RetryPolicy<Void> buildRetryPolicy() {
            return RetryPolicy.<Void>builder()
                .withMaxRetries(MAX_RECONNECT_ATTEMPTS)
                .withBackoff(INITIAL_RECONNECT_DELAY, MAX_RECONNECT_DELAY)
                .onRetry(e -> {
                    if (!stateMachine.isClosed()) {
                        RetryContext ctx = new RetryContext(
                            RetryContext.RetryType.RESUME,
                            e.getAttemptCount(),
                            MAX_RECONNECT_ATTEMPTS,
                            null,
                            e.getLastException());
                        Exceptions.quietly(() -> listener.onRetry(ctx));
                    }
                })
                .build();
        }

        private WatchCreateRequest buildCreateRequest() {
            WatchCreateRequest.Builder builder = WatchCreateRequest.newBuilder()
                .setKey(Util.prefixNamespace(key, namespace))
                .setPrevKv(option.isPrevKV())
                .setProgressNotify(option.isProgressNotify())
                .setStartRevision(revision);

            option.getEndKey()
                .map(endKey -> Util.prefixNamespaceToRangeEnd(endKey, namespace))
                .ifPresent(builder::setRangeEnd);

            if (option.getEndKey().isEmpty() && option.isPrefix()) {
                ByteSequence endKey = OptionsUtil.prefixEndOf(key);
                builder.setRangeEnd(Util.prefixNamespaceToRangeEnd(endKey, namespace));
            }

            if (option.isNoDelete()) {
                builder.addFilters(WatchCreateRequest.FilterType.NODELETE);
            }

            if (option.isNoPut()) {
                builder.addFilters(WatchCreateRequest.FilterType.NOPUT);
            }

            return builder.build();
        }

        private void sendCancelRequest() {
            WatchStream ws = watchStream;
            if (ws == null || !ws.isWriteStreamReady()) {
                return;
            }

            WatchCancelRequest cancel = WatchCancelRequest.newBuilder().build();
            ws.send(WatchRequest.newBuilder().setCancelRequest(cancel).build());
        }

        private void notifyListener(io.etcd.jetcd.api.WatchResponse response, boolean withNamespace) {
            if (stateMachine.isClosed()) {
                return;
            }

            WatchResponse watchResponse = withNamespace
                ? new WatchResponse(response, namespace)
                : new WatchResponse(response);

            Exceptions.quietly(() -> listener.onNext(watchResponse));
        }

        private void updateRevision(long newRevision) {
            revision = Math.max(revision, newRevision);
        }
    }
}
