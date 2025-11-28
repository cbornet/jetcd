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

import com.google.common.base.Strings;
import dev.failsafe.RetryPolicy;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.api.WatchCancelRequest;
import io.etcd.jetcd.api.WatchCreateRequest;
import io.etcd.jetcd.api.WatchGrpcClient;
import io.etcd.jetcd.api.WatchProgressRequest;
import io.etcd.jetcd.api.WatchRequest;
import io.etcd.jetcd.common.exception.ErrorCode;
import io.etcd.jetcd.common.exception.EtcdException;
import io.etcd.jetcd.common.exception.Exceptions;
import io.etcd.jetcd.common.vertx.Failsafe;
import io.etcd.jetcd.options.OptionsUtil;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.resolver.ServiceResolver;
import io.etcd.jetcd.support.Util;
import io.etcd.jetcd.watch.RetryContext;
import io.etcd.jetcd.watch.WatchResponse;
import io.vertx.core.Vertx;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import io.vertx.grpc.common.GrpcStatus;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.etcd.jetcd.common.exception.ErrorCode.FAILED_PRECONDITION;
import static io.etcd.jetcd.common.exception.ErrorCode.INTERNAL;
import static io.etcd.jetcd.common.exception.ErrorCode.OUT_OF_RANGE;
import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.newClosedWatchClientException;
import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.newCompactedException;
import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.newEtcdException;
import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.toEtcdException;

/**
 * Watch implementation where each watcher manages its own dedicated gRPC stream.
 */
final class WatchImpl extends Impl implements Watch {
    private final AtomicBoolean closed;
    private final List<Watcher> watchers;
    private final ByteSequence namespace;

    WatchImpl(ClientConnectionManager connectionManager) {
        super(connectionManager);
        this.closed = new AtomicBoolean();
        this.watchers = new CopyOnWriteArrayList<>();
        this.namespace = connectionManager.getNamespace();
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
            connectionManager(),
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
                        .toArray(CompletableFuture[]::new)
                );

                f.get(15, TimeUnit.SECONDS);
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
     */
    private static final class WatcherImpl implements Watch.Watcher {
        private static final int MAX_RECONNECT_ATTEMPTS = 10;
        private static final long INITIAL_RECONNECT_DELAY_MS = 500;
        private static final long MAX_RECONNECT_DELAY_MS = 30000;

        private final ByteSequence key;
        private final ByteSequence namespace;
        private final WatchOption option;
        private final Watch.Listener listener;
        private final ClientConnectionManager connectionManager;
        private final Vertx vertx;
        private final java.util.function.Consumer<WatcherImpl> onClose;
        private final Object watcherLock;

        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean connected = new AtomicBoolean();

        private volatile WriteStream<WatchRequest> writeStream;
        private volatile ReadStream<io.etcd.jetcd.api.WatchResponse> readStream;
        private volatile WatchGrpcClient grpcClient;
        private volatile Long pendingTimerId;
        private long revision;

        WatcherImpl(
            ByteSequence key,
            ByteSequence namespace,
            WatchOption option,
            Watch.Listener listener,
            ClientConnectionManager connectionManager,
            java.util.function.Consumer<WatcherImpl> onClose) {

            this.key = key;
            this.namespace = namespace;
            this.option = option;
            this.listener = listener;
            this.revision = option.getRevision();
            this.connectionManager = connectionManager;
            this.vertx = connectionManager.vertx();
            this.onClose = onClose;
            this.watcherLock = new Object();

            connect().exceptionally(error -> {
                notifyError(toEtcdException(error));
                return null;
            });
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            Watch.Listener callbackListener = null;
            java.util.function.Consumer<WatcherImpl> closeCallback = null;

            synchronized (watcherLock) {
                if (closed.compareAndSet(false, true)) {
                    if (pendingTimerId != null) {
                        Exceptions.quietly(() -> vertx.cancelTimer(pendingTimerId));
                        pendingTimerId = null;
                    }

                    callbackListener = listener;
                    closeCallback = onClose;
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            }

            sendCancelRequest();
            disconnect();

            if (callbackListener != null) {
                Watch.Listener finalListener = callbackListener;
                Exceptions.quietly(finalListener::onCompleted);
            }

            if (closeCallback != null) {
                java.util.function.Consumer<WatcherImpl> finalCallback = closeCallback;
                Exceptions.quietly(() -> finalCallback.accept(this));
            }

            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void requestProgress() {
            if (closed.get() || !connected.get()) {
                return;
            }

            synchronized (watcherLock) {
                if (closed.get() || !connected.get()) {
                    return;
                }

                WriteStream<WatchRequest> ws = writeStream;
                if (ws != null) {
                    Exceptions.quietly(() -> {
                        WatchProgressRequest progress = WatchProgressRequest.newBuilder().build();
                        ws.write(WatchRequest.newBuilder().setProgressRequest(progress).build());
                    });
                }
            }
        }

        private CompletableFuture<Void> connect() {
            CompletableFuture<Void> connectionFuture = new CompletableFuture<>();

            synchronized (watcherLock) {
                if (connected.get() || closed.get()) {
                    connectionFuture.complete(null);
                    return connectionFuture;
                }
            }

            vertx.runOnContext(v -> {
                try {
                    grpcClient = createWatchClient();

                    io.vertx.core.Future<ReadStream<io.etcd.jetcd.api.WatchResponse>> watchFuture = grpcClient
                        .watch((ws, err) -> {
                            if (err != null) {
                                handleConnectError(err, connectionFuture);
                            } else {
                                synchronized (watcherLock) {
                                    if (!closed.get()) {
                                        writeStream = ws;
                                        Exceptions.quietly(() -> ws.write(WatchRequest.newBuilder()
                                            .setCreateRequest(buildCreateRequest())
                                            .build()));
                                    }
                                }
                            }
                        });

                    watchFuture.onComplete(ar -> {
                        if (ar.succeeded()) {
                            synchronized (watcherLock) {
                                if (!closed.get()) {
                                    readStream = ar.result();
                                    readStream.handler(this::onNext);
                                    readStream.endHandler(v2 -> onStreamEnded());
                                    readStream.exceptionHandler(this::onStreamError);
                                    connected.set(true);
                                }
                            }
                            connectionFuture.complete(null);
                        } else {
                            handleConnectError(ar.cause(), connectionFuture);
                        }
                    });
                } catch (Exception e) {
                    handleConnectError(e, connectionFuture);
                }
            });

            return connectionFuture;
        }

        private void disconnect() {
            WriteStream<WatchRequest> ws;
            ReadStream<io.etcd.jetcd.api.WatchResponse> rs;

            synchronized (watcherLock) {
                connected.set(false);
                ws = writeStream;
                rs = readStream;
                writeStream = null;
                readStream = null;
                grpcClient = null;
            }

            Exceptions.quietly(() -> {
                if (ws != null)
                    ws.end();
            });
            Exceptions.quietly(() -> {
                if (rs != null) {
                    rs.handler(null);
                    rs.endHandler(null);
                    rs.exceptionHandler(null);
                }
            });
        }

        private void reconnect() {
            if (closed.get()) {
                return;
            }

            disconnect();
            connect().exceptionally(error -> {
                notifyError(toEtcdException(error));
                return null;
            });
        }

        private void scheduleReconnect() {
            if (closed.get()) {
                return;
            }

            RetryPolicy<Void> retryPolicy = RetryPolicy.<Void> builder()
                .withMaxRetries(MAX_RECONNECT_ATTEMPTS)
                .withBackoff(Duration.ofMillis(INITIAL_RECONNECT_DELAY_MS), Duration.ofMillis(MAX_RECONNECT_DELAY_MS))
                .onRetry(e -> {
                    RetryContext ctx = new RetryContext(
                        RetryContext.RetryType.RESUME,
                        e.getAttemptCount(),
                        MAX_RECONNECT_ATTEMPTS,
                        null,
                        e.getLastException());

                    Watch.Listener callbackListener;
                    synchronized (watcherLock) {
                        if (closed.get()) {
                            return;
                        }
                        callbackListener = listener;
                    }

                    Exceptions.quietly(() -> callbackListener.onRetry(ctx));
                })
                .onRetriesExceeded(e -> {
                    EtcdException error = newEtcdException(
                        ErrorCode.UNAVAILABLE,
                        "Watch stream failed after " + MAX_RECONNECT_ATTEMPTS + " attempts");
                    notifyError(error);
                })
                .build();

            Failsafe.runAsync(vertx, this::reconnect, retryPolicy);
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
            if (!connected.get()) {
                return;
            }

            synchronized (watcherLock) {
                WriteStream<WatchRequest> ws = writeStream;
                if (ws != null && connected.get()) {
                    Exceptions.quietly(() -> {
                        WatchCancelRequest cancel = WatchCancelRequest.newBuilder().build();
                        ws.write(WatchRequest.newBuilder().setCancelRequest(cancel).build());
                    });
                }
            }
        }

        private void onNext(io.etcd.jetcd.api.WatchResponse response) {
            if (closed.get()) {
                return;
            }

            if (handleAuthError(response)) {
                return;
            }

            if (handleWatchCreated(response)) {
                return;
            }

            if (handleWatchCanceled(response)) {
                return;
            }

            if (handleProgressNotify(response)) {
                return;
            }

            handleEvents(response);
        }

        private void onStreamEnded() {
            if (closed.get()) {
                return;
            }

            scheduleReconnect();
        }

        private void onStreamError(Throwable error) {
            if (closed.get()) {
                return;
            }

            notifyError(toEtcdException(error));
            scheduleReconnect();
        }

        private void handleConnectError(Throwable error, CompletableFuture<Void> future) {
            future.completeExceptionally(error);

            if (!closed.get()) {
                notifyError(toEtcdException(error));
                scheduleReconnect();
            }
        }

        private void notifyError(EtcdException error) {
            Watch.Listener callbackListener;

            synchronized (watcherLock) {
                if (closed.get()) {
                    return;
                }
                callbackListener = listener;
            }

            Exceptions.quietly(() -> callbackListener.onError(error));
        }

        private void notifyListener(io.etcd.jetcd.api.WatchResponse response, boolean withNamespace) {
            Watch.Listener callbackListener;

            synchronized (watcherLock) {
                if (closed.get()) {
                    return;
                }
                callbackListener = listener;
            }

            WatchResponse watchResponse = withNamespace
                ? new WatchResponse(response, namespace)
                : new WatchResponse(response);

            Exceptions.quietly(() -> callbackListener.onNext(watchResponse));
        }

        private void updateRevision(long newRevision) {
            revision = Math.max(revision, newRevision);
        }

        private void updateRevisionFromEvents(io.etcd.jetcd.api.WatchResponse response) {
            if (response.getEventsCount() > 0) {
                revision = response.getEvents(response.getEventsCount() - 1)
                    .getKv()
                    .getModRevision() + 1;
            }
        }

        private boolean handleAuthError(io.etcd.jetcd.api.WatchResponse response) {
            if (!response.getCreated() || !response.getCanceled()) {
                return false;
            }

            String cancelReason = response.getCancelReason();
            if (cancelReason.isEmpty()) {
                return false;
            }

            if (cancelReason.contains("etcdserver: permission denied") ||
                cancelReason.contains("etcdserver: invalid auth token")) {

                connectionManager.authCredential().refresh();
                handleError(toEtcdException(GrpcStatus.CANCELLED), true);
                return true;
            }

            return false;
        }

        private boolean handleWatchCreated(io.etcd.jetcd.api.WatchResponse response) {
            if (!response.getCreated()) {
                return false;
            }

            if (response.getWatchId() == -1) {
                notifyError(newEtcdException(INTERNAL, "etcd server failed to create watch id"));
                return true;
            }

            updateRevision(response.getHeader().getRevision());

            if (option.isCreatedNotify()) {
                notifyListener(response, false);
            }

            return true;
        }

        private boolean handleWatchCanceled(io.etcd.jetcd.api.WatchResponse response) {
            if (!response.getCanceled()) {
                return false;
            }

            Throwable error;
            String reason = response.getCancelReason();

            if (response.getCompactRevision() != 0) {
                error = newCompactedException(response.getCompactRevision());
            } else if (Strings.isNullOrEmpty(reason)) {
                error = newEtcdException(OUT_OF_RANGE,
                    "etcdserver: mvcc: required revision is a future revision");
            } else {
                error = newEtcdException(FAILED_PRECONDITION, reason);
            }

            handleError(toEtcdException(error), false);
            return true;
        }

        private boolean handleProgressNotify(io.etcd.jetcd.api.WatchResponse response) {
            if (WatchResponse.isProgressNotify(response)) {
                notifyListener(response, false);
                updateRevision(response.getHeader().getRevision());
                return true;
            }

            if (response.getEventsCount() == 0 && option.isProgressNotify()) {
                notifyListener(response, true);
                revision = response.getHeader().getRevision();
                return true;
            }

            return false;
        }

        private boolean handleEvents(io.etcd.jetcd.api.WatchResponse response) {
            if (response.getEventsCount() == 0) {
                return false;
            }

            notifyListener(response, true);
            updateRevisionFromEvents(response);
            return true;
        }

        private void handleError(EtcdException etcdException, boolean shouldReschedule) {
            notifyError(etcdException);

            if (shouldReschedule) {
                if (etcdException.getMessage().contains("etcdserver: permission denied")) {
                    connectionManager.authCredential().refresh();
                }

                reschedule();
            } else {
                close();
            }
        }

        private void reschedule() {
            synchronized (watcherLock) {
                if (closed.get()) {
                    return;
                }
            }

            long timerId = vertx.setTimer(500, id -> {
                boolean shouldResume;

                synchronized (watcherLock) {
                    if (pendingTimerId != null && pendingTimerId.equals(id)) {
                        pendingTimerId = null;
                        shouldResume = !closed.get();
                    } else {
                        shouldResume = false;
                    }
                }

                if (shouldResume) {
                    Exceptions.quietly(() -> reconnect());
                }
            });

            synchronized (watcherLock) {
                if (!closed.get()) {
                    pendingTimerId = timerId;
                } else {
                    Exceptions.quietly(() -> vertx.cancelTimer(timerId));
                }
            }
        }

        private WatchGrpcClient createWatchClient() {
            ServiceResolver<?> serviceResolver = connectionManager.getServiceResolver();

            return WatchGrpcClient.create(
                connectionManager.getAuthenticatedGrpcClient(),
                serviceResolver.getTarget(io.vertx.core.net.SocketAddress.class)
            );
        }
    }
}
