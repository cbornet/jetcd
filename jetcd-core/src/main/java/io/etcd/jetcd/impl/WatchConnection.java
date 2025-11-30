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
import io.etcd.jetcd.common.exception.EtcdException;
import io.etcd.jetcd.common.exception.Exceptions;
import io.etcd.jetcd.common.vertx.Failsafe;
import io.etcd.jetcd.grpc.GrpcService;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.RetryContext;
import io.etcd.jetcd.watch.WatchResponse;
import io.etcd.jetcd.watch.WatchState;
import io.vertx.core.Vertx;
import io.vertx.grpc.common.GrpcStatus;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.toEtcdException;

/**
 * Individual watcher that owns its dedicated gRPC stream.
 * Uses WatchStateMachine for lifecycle management and WatchStream for gRPC stream handling.
 */
final class WatchConnection implements Watch.Watcher, WatchStream.Handler {
    private final ByteSequence key;
    private final ByteSequence namespace;
    private final WatchOption option;
    private final Watch.Listener listener;
    private final GrpcService grpcService;
    private final Vertx vertx;
    private final Consumer<WatchConnection> onClose;
    private final WatchResponseProcessor responseProcessor;
    private final WatchStateMachine stateMachine;
    private final CompletableFuture<Void> readyFuture = new CompletableFuture<>();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    // Event loop only fields
    private WatchStream watchStream;
    private CompletableFuture<Void> reconnectFuture;
    private boolean pendingProgressRequest;
    private long revision;

    WatchConnection(
            ByteSequence key,
            ByteSequence namespace,
            WatchOption option,
            Watch.Listener listener,
            GrpcService grpcService,
            Consumer<WatchConnection> onClose) {

        this.key = key;
        this.namespace = namespace;
        this.option = option;
        this.listener = listener;
        this.revision = option.getRevision();
        this.grpcService = grpcService;
        this.vertx = grpcService.vertx();
        this.onClose = onClose;
        this.responseProcessor = new WatchResponseProcessor(option);
        this.stateMachine = new WatchStateMachine(vertx, new WatchStateMachine.Handler() {
            @Override
            public void onConnect() {
                handleConnect();
            }

            @Override
            public void onSubscribe() {
                handleSubscribe();
            }

            @Override
            public void onReady() {
                handleReady();
            }

            @Override
            public void onReconnect() {
                handleReconnect();
            }

            @Override
            public void onClose() {
                handleClose();
            }

            @Override
            public void onError(Throwable error) {
                handleError(error);
            }

            @Override
            public void onStateChange(WatchStateMachine.State oldState, WatchStateMachine.State newState) {
                handleStateChange(oldState, newState);
            }
        });

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

            stream.send(WatchRequestFactory.progressRequest());
            pendingProgressRequest = false;
        });
    }

    // State machine handlers

    private void handleConnect() {
        watchStream = new WatchStream(grpcService);
        watchStream.connect(this).onComplete(ar -> {
            if (ar.failed()) {
                stateMachine.streamError(ar.cause());
            }
        });
    }

    private void handleSubscribe() {
        WatchStream stream = watchStream;
        if (stream != null) {
            stream.send(WatchRequestFactory.createRequest(key, namespace, option, revision));
        }
    }

    private void handleReady() {
        readyFuture.complete(null);

        // Send pending progress request if one was queued
        if (pendingProgressRequest) {
            pendingProgressRequest = false;
            WatchStream stream = watchStream;
            if (stream != null && stream.isWriteStreamReady()) {
                stream.send(WatchRequestFactory.progressRequest());
            }
        }
    }

    private void handleReconnect() {
        if (reconnectFuture != null && !reconnectFuture.isDone()) {
            return;
        }

        reconnectFuture = Failsafe.runAsync(vertx, this::disconnect, buildRetryPolicy())
            .whenComplete((result, error) -> {
                reconnectFuture = null;
                if (error == null) {
                    stateMachine.reconnectSucceeded();
                } else {
                    stateMachine.reconnectFailed(error);
                }
            });
    }

    private void handleClose() {
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

    private void handleError(Throwable error) {
        EtcdException etcdException = toEtcdException(error);
        Exceptions.quietly(() -> listener.onError(etcdException));
        readyFuture.completeExceptionally(error);
    }

    private void handleStateChange(WatchStateMachine.State oldState, WatchStateMachine.State newState) {
        WatchState oldPublicState = toPublicState(oldState);
        WatchState newPublicState = toPublicState(newState);
        Exceptions.quietly(() -> listener.onStateChange(oldPublicState, newPublicState));
    }

    private static WatchState toPublicState(WatchStateMachine.State state) {
        return switch (state) {
            case CONNECTING -> WatchState.CONNECTING;
            case SUBSCRIBING -> WatchState.SUBSCRIBING;
            case WATCHING -> WatchState.WATCHING;
            case RECONNECTING -> WatchState.RECONNECTING;
            case CLOSED -> WatchState.CLOSED;
        };
    }

    // WatchStream.Handler implementation

    @Override
    public void onMessage(io.etcd.jetcd.api.WatchResponse response) {
        if (stateMachine.isClosed()) {
            return;
        }

        switch (responseProcessor.process(response)) {
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
            .withMaxRetries(option.getMaxReconnectAttempts())
            .withBackoff(option.getInitialReconnectDelay(), option.getMaxReconnectDelay())
            .onRetry(e -> {
                if (stateMachine.isClosed()) {
                    return;
                }

                RetryContext ctx = RetryContext.of(RetryContext.RetryType.RESUME)
                    .attemptCount(e.getAttemptCount())
                    .maxAttempts(option.getMaxReconnectAttempts())
                    .cause(e.getLastException())
                    .build();

                Exceptions.quietly(() -> listener.onRetry(ctx));
            })
            .build();
    }

    private void sendCancelRequest() {
        WatchStream ws = watchStream;
        if (ws == null || !ws.isWriteStreamReady()) {
            return;
        }

        ws.send(WatchRequestFactory.cancelRequest());
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

