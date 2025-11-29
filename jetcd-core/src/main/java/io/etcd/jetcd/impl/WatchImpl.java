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
import io.etcd.jetcd.common.exception.ErrorCode;
import io.etcd.jetcd.common.exception.EtcdException;
import io.etcd.jetcd.common.exception.Exceptions;
import io.etcd.jetcd.common.vertx.Failsafe;
import io.etcd.jetcd.options.OptionsUtil;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.support.Errors;
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
import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.newEtcdException;
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
     * Uses WatchStream for stream lifecycle management.
     */
    private static final class WatcherImpl implements Watch.Watcher, WatchStream.Handler {
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

        // Thread-safe flags
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean pendingProgressRequest = new AtomicBoolean();

        // Volatile for cross-thread visibility
        private volatile WatchStream watchStream;

        // Accessed from event loop only (protected by runOnContext dispatch)
        private CompletableFuture<Void> reconnectFuture;
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

            connect();
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            if (!closed.compareAndSet(false, true)) {
                return CompletableFuture.completedFuture(null);
            }

            // Cancel pending reconnect on event loop
            vertx.runOnContext(v -> {
                if (reconnectFuture != null && !reconnectFuture.isDone()) {
                    reconnectFuture.cancel(true);
                    reconnectFuture = null;
                }
            });

            sendCancelRequest();
            disconnect();

            // Callbacks called synchronously
            Exceptions.quietly(listener::onCompleted);
            Exceptions.quietly(() -> onClose.accept(this));

            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void requestProgress() {
            if (closed.get()) {
                return;
            }

            WatchStream ws = watchStream;
            if (ws == null || !ws.isWriteStreamReady()) {
                pendingProgressRequest.set(true);
                return;
            }

            vertx.runOnContext(v -> {
                if (!closed.get()) {
                    WatchStream stream = watchStream;
                    if (stream != null && stream.isWriteStreamReady()) {
                        WatchProgressRequest progress = WatchProgressRequest.newBuilder().build();
                        stream.send(WatchRequest.newBuilder().setProgressRequest(progress).build());
                        pendingProgressRequest.set(false);
                    }
                }
            });
        }

        // WatchStream.Handler implementation

        @Override
        public void onMessage(io.etcd.jetcd.api.WatchResponse response) {
            if (closed.get()) {
                return;
            }

            WatchResponseProcessor.Result result = responseProcessor.process(response);

            switch (result) {
                case WatchResponseProcessor.Result.AuthError() -> {
                    grpcService.auth().refreshToken();
                    handleError(toEtcdException(GrpcStatus.CANCELLED), true);
                }
                case WatchResponseProcessor.Result.Created(long rev, boolean shouldNotify) -> {
                    updateRevision(rev);
                    if (shouldNotify) {
                        notifyListener(response, false);
                    }
                }
                case WatchResponseProcessor.Result.Canceled(Throwable error) -> {
                    handleError(toEtcdException(error), false);
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
            if (closed.get()) {
                return;
            }
            reconnect();
        }

        @Override
        public void onError(Throwable error) {
            if (closed.get()) {
                return;
            }
            notifyError(toEtcdException(error));
            reconnect();
        }

        @Override
        public void onWriteStreamReady(WatchStream stream) {
            if (!closed.get()) {
                stream.send(WatchRequest.newBuilder()
                    .setCreateRequest(buildCreateRequest())
                    .build());
            }
        }

        private void connect() {
            if (closed.get()) {
                return;
            }

            WatchStream ws = watchStream;
            if (ws != null && ws.isConnected()) {
                return;
            }

            watchStream = new WatchStream(grpcService);
            watchStream.connect(this).onComplete(ar -> {
                if (ar.succeeded() && !closed.get()) {
                    // Send pending progress request if one was queued
                    if (pendingProgressRequest.getAndSet(false)) {
                        WatchStream stream = watchStream;
                        if (stream != null) {
                            WatchProgressRequest progress = WatchProgressRequest.newBuilder().build();
                            stream.send(WatchRequest.newBuilder().setProgressRequest(progress).build());
                        }
                    }
                } else if (ar.failed() && !closed.get()) {
                    notifyError(toEtcdException(ar.cause()));
                    reconnect();
                }
            });
        }

        private void disconnect() {
            if (closed.get()) {
                return;
            }

            pendingProgressRequest.set(false);

            WatchStream ws = watchStream;
            watchStream = null;

            if (ws != null) {
                ws.disconnect();
            }
        }

        private void reconnect() {
            // Called from event loop (onEnd/onError handlers)
            if (closed.get() || (reconnectFuture != null && !reconnectFuture.isDone())) {
                return;
            }

            reconnectFuture = Failsafe.runAsync(vertx, () -> {
                disconnect();
                connect();
            }, buildRetryPolicy()).whenComplete((result, error) -> {
                // Runs on event loop via Failsafe scheduler
                reconnectFuture = null;
            });
        }

        private RetryPolicy<Void> buildRetryPolicy() {
            return RetryPolicy.<Void>builder()
                .withMaxRetries(MAX_RECONNECT_ATTEMPTS)
                .withBackoff(INITIAL_RECONNECT_DELAY, MAX_RECONNECT_DELAY)
                .onRetry(e -> {
                    if (!closed.get()) {
                        RetryContext ctx = new RetryContext(
                            RetryContext.RetryType.RESUME,
                            e.getAttemptCount(),
                            MAX_RECONNECT_ATTEMPTS,
                            null,
                            e.getLastException());
                        Exceptions.quietly(() -> listener.onRetry(ctx));
                    }
                })
                .onRetriesExceeded(e -> {
                    if (!closed.get()) {
                        notifyError(newEtcdException(ErrorCode.UNAVAILABLE,
                            "Watch stream failed after " + MAX_RECONNECT_ATTEMPTS + " attempts"));
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

        private void notifyError(EtcdException error) {
            if (closed.get()) {
                return;
            }

            Exceptions.quietly(() -> listener.onError(error));
        }

        private void notifyListener(io.etcd.jetcd.api.WatchResponse response, boolean withNamespace) {
            if (closed.get()) {
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

        private void handleError(EtcdException etcdException, boolean shouldReconnect) {
            notifyError(etcdException);

            if (shouldReconnect) {
                if (Errors.isPermissionDenied(etcdException.getMessage())) {
                    grpcService.auth().refreshToken();
                }

                reconnect();
            } else {
                close();
            }
        }
    }
}
