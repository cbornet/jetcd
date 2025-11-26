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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.api.WatchCancelRequest;
import io.etcd.jetcd.api.WatchCreateRequest;
import io.etcd.jetcd.api.WatchGrpcClient;
import io.etcd.jetcd.api.WatchProgressRequest;
import io.etcd.jetcd.api.WatchRequest;
import io.etcd.jetcd.api.WatchResponse;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.support.ReferenceCount;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

/**
 * Manages a single shared bidirectional gRPC stream for all watchers.
 * Uses lazy connection (connects on first watcher) and ref counting (disconnects on last watcher).
 * Inspired by Apache Camel's ReferenceCount pattern.
 */
public final class WatchStreamManager implements WatchStream {
    private static final Logger LOG = LoggerFactory.getLogger(WatchStreamManager.class);
    private static final int MAX_PENDING_REQUESTS = 1000;
    private static final int MAX_RECONNECT_ATTEMPTS = 10;
    private static final long INITIAL_RECONNECT_DELAY_MS = 500;
    private static final long MAX_RECONNECT_DELAY_MS = 30000;

    private final ClientConnectionManager connectionManager;
    private final ByteSequence namespace;
    private final ListeningScheduledExecutorService executor;
    private final Object parentLock;
    private final java.util.concurrent.atomic.AtomicBoolean parentClosed;
    private final java.util.List<Watch.Watcher> parentWatchers;

    private volatile WriteStream<WatchRequest> writeStream;
    private volatile ReadStream<WatchResponse> readStream;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final Object streamLock = new Object();

    private final AtomicLong watchIdGenerator = new AtomicLong(1);
    private final ConcurrentHashMap<Long, WatcherImpl> watcherMap = new ConcurrentHashMap<>();
    private final ReferenceCount refCount;
    private final List<WatchCreateRequest> pendingRequests = new CopyOnWriteArrayList<>();
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);

    public WatchStreamManager(
            ClientConnectionManager connectionManager,
            ByteSequence namespace,
            ListeningScheduledExecutorService executor,
            Object parentLock,
            java.util.concurrent.atomic.AtomicBoolean parentClosed,
            java.util.List<Watch.Watcher> parentWatchers) {
        this.connectionManager = connectionManager;
        this.namespace = namespace;
        this.executor = executor;
        this.parentLock = parentLock;
        this.parentClosed = parentClosed;
        this.parentWatchers = parentWatchers;

        // Ref count with callbacks for lazy connect/disconnect
        this.refCount = ReferenceCount.on(
            this::connect,      // Called when first watcher added (0 → 1)
            this::disconnect    // Called when last watcher removed (1 → 0)
        );
    }

    public WatcherImpl createWatcher(ByteSequence key, WatchOption option, Watch.Listener listener) {
        long watchId = watchIdGenerator.getAndIncrement();

        // Create callback for watcher close
        Runnable onClose = () -> {
            onWatcherClosed(watchId);
            parentWatchers.remove(watcherMap.get(watchId));
        };

        WatcherImpl watcher = new WatcherImpl(
            this,
            watchId,
            key,
            namespace,
            option,
            listener,
            parentLock,
            parentClosed,
            connectionManager,
            executor,
            onClose
        );

        // Register and increment ref count (lazy connects if first)
        watcherMap.put(watchId, watcher);

        LOG.debug("Creating watcher: watch_id={}, key={}", watchId, key);

        refCount.retain();

        // Initiate watch on server
        watcher.resume();

        return watcher;
    }

    void onWatcherClosed(long watchId) {
        WatcherImpl removed = watcherMap.remove(watchId);

        if (removed != null) {
            // Send cancel if connected
            if (connected.get()) {
                sendCancelRequest(watchId);
            }

            // Decrement ref count (lazy disconnects if last)
            try {
                refCount.release();
                LOG.debug("Closed watcher: watch_id={}, refCount={}", watchId, refCount.get());
            } catch (Exception e) {
                LOG.error("Failed to release ref count for watch_id={}, but watcher removed from map", watchId, e);
            }
        }
    }

    private void connect() {
        synchronized (streamLock) {
            if (connected.get()) {
                LOG.debug("Already connected, skipping connect()");
                return;
            }

            LOG.debug("Connecting watch stream (first watcher added), attempt {}", reconnectAttempts.get() + 1);

            WatchGrpcClient client;
            try {
                client = createWatchClient();
            } catch (Exception e) {
                LOG.error("Failed to create WatchGrpcClient", e);
                handleConnectError(e);
                return;
            }

            // Execute on Vert.x context to ensure async callbacks work
            io.vertx.core.Vertx vertx = connectionManager.vertx();

            vertx.runOnContext(v -> {
                try {
                    io.vertx.core.Future<ReadStream<WatchResponse>> watchFuture = client.watch((ws, err) -> {
                        if (err != null) {
                            LOG.error("Failed to establish write stream", err);
                            handleConnectError(err);
                        } else {
                            LOG.debug("Write stream established");
                            writeStream = ws;

                            // Send pending requests immediately while write stream is ready
                            // The Future won't complete until server responds, which requires us to send first!
                            LOG.debug("Sending {} pending watch create requests", pendingRequests.size());
                            for (WatchCreateRequest req : pendingRequests) {
                                ws.write(WatchRequest.newBuilder().setCreateRequest(req).build());
                            }
                            pendingRequests.clear();
                        }
                    });

                    watchFuture.onComplete(ar -> {
                        if (ar.succeeded()) {
                            LOG.debug("Read stream established");
                            readStream = ar.result();
                            readStream.handler(this::routeResponse);
                            readStream.endHandler(v2 -> onStreamEnded());
                            readStream.exceptionHandler(this::onStreamError);
                            connected.set(true);
                            reconnectAttempts.set(0);
                            LOG.debug("Watch stream connected successfully");
                        } else {
                            LOG.error("Failed to complete stream setup", ar.cause());
                            handleConnectError(ar.cause());
                        }
                    });
                } catch (Exception e) {
                    LOG.error("Exception during client.watch() call", e);
                    handleConnectError(e);
                }
            });
        }
    }

    private void disconnect() {
        synchronized (streamLock) {
            if (!connected.get()) {
                return;
            }

            LOG.debug("Disconnecting watch stream (last watcher removed)");

            connected.set(false);

            if (writeStream != null) {
                try {
                    writeStream.end();
                } catch (Exception e) {
                    LOG.warn("Error ending write stream", e);
                }
                writeStream = null;
            }

            readStream = null;

            // Reset watch_id counter for fresh start on next connection
            watchIdGenerator.set(1);
            pendingRequests.clear();
            reconnectAttempts.set(0);

            LOG.debug("Watch stream disconnected, watch_id counter and reconnect attempts reset");
        }
    }

    public void forceDisconnect() {
        synchronized (streamLock) {
            if (connected.get()) {
                LOG.info("Force disconnecting watch stream");
                connected.set(false);

                if (writeStream != null) {
                    try {
                        writeStream.end();
                    } catch (Exception e) {
                        LOG.warn("Error ending write stream during force disconnect", e);
                    }
                    writeStream = null;
                }

                readStream = null;

                // Reset state for clean shutdown
                watchIdGenerator.set(1);
                pendingRequests.clear();
            }
        }
    }

    private void reconnect() {
        synchronized (streamLock) {
            if (refCount.get() == 0) {
                LOG.debug("No active watchers, skipping reconnect");
                reconnectAttempts.set(0);
                return;
            }

            int currentAttempt = reconnectAttempts.incrementAndGet();
            if (currentAttempt > MAX_RECONNECT_ATTEMPTS) {
                LOG.error("Max reconnect attempts ({}) reached, giving up. Notifying all watchers.", MAX_RECONNECT_ATTEMPTS);
                notifyWatchersOfPermanentFailure();
                return;
            }

            LOG.info("Reconnecting watch stream, attempt {}/{}", currentAttempt, MAX_RECONNECT_ATTEMPTS);
            disconnect();
            connect();

            if (connected.get()) {
                recreateAllWatches();
            } else {
                scheduleReconnect();
            }
        }
    }

    private void recreateAllWatches() {
        LOG.info("Recreating {} watches after reconnect", watcherMap.size());
        watcherMap.values().forEach(watcher -> {
            if (!watcher.isClosed()) {
                watcher.resume();
            }
        });
    }

    @Override
    public void sendCreateRequest(WatchCreateRequest req) {
        if (!connected.get()) {
            if (pendingRequests.size() >= MAX_PENDING_REQUESTS) {
                LOG.error("Pending requests queue full ({} requests), notifying watcher for watch_id={}",
                    MAX_PENDING_REQUESTS, req.getWatchId());

                WatcherImpl watcher = watcherMap.get(req.getWatchId());
                if (watcher != null) {
                    io.etcd.jetcd.common.exception.EtcdException queueFullError =
                        io.etcd.jetcd.common.exception.EtcdExceptionFactory.newEtcdException(
                            io.etcd.jetcd.common.exception.ErrorCode.RESOURCE_EXHAUSTED,
                            "Pending watch requests queue is full (" + MAX_PENDING_REQUESTS + ")");
                    watcher.notifyError(queueFullError);
                }
                return;
            }
            LOG.debug("Stream not connected yet, queueing create request for watch_id={} (queue size: {})",
                req.getWatchId(), pendingRequests.size() + 1);
            pendingRequests.add(req);
            return;
        }

        WriteStream<WatchRequest> ws = writeStream;
        if (ws != null) {
            LOG.debug("Sending create request for watch_id={}", req.getWatchId());
            ws.write(WatchRequest.newBuilder().setCreateRequest(req).build());
        } else {
            LOG.error("WriteStream is null despite connected=true for watch_id={}", req.getWatchId());
        }
    }

    @Override
    public void sendCancelRequest(long watchId) {
        if (!connected.get()) {
            return;
        }

        WriteStream<WatchRequest> ws = writeStream;
        if (ws != null) {
            WatchCancelRequest cancel = WatchCancelRequest.newBuilder()
                .setWatchId(watchId)
                .build();
            ws.write(WatchRequest.newBuilder().setCancelRequest(cancel).build());
        }
    }

    @Override
    public void sendProgressRequest() {
        if (!connected.get()) {
            return;
        }

        WriteStream<WatchRequest> ws = writeStream;
        if (ws != null) {
            WatchProgressRequest progress = WatchProgressRequest.newBuilder().build();
            ws.write(WatchRequest.newBuilder().setProgressRequest(progress).build());
        }
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    private void routeResponse(WatchResponse response) {
        long watchId = response.getWatchId();

        LOG.debug("Received WatchResponse for watch_id={}, created={}, canceled={}, events={}",
            watchId, response.getCreated(), response.getCanceled(), response.getEventsCount());

        // Handle progress notify for all watchers (watch_id=-1)
        if (watchId == -1) {
            LOG.debug("Broadcasting progress response to all {} watchers", watcherMap.size());
            for (WatcherImpl watcher : watcherMap.values()) {
                if (!watcher.isClosed()) {
                    try {
                        // Create a new response with the watcher's actual watch_id
                        WatchResponse watcherResponse = response.toBuilder()
                            .setWatchId(watcher.getWatchId())
                            .build();
                        watcher.onNext(watcherResponse);
                    } catch (Exception e) {
                        LOG.error("Error broadcasting progress to watch_id={}", watcher.getWatchId(), e);
                    }
                }
            }
            return;
        }

        WatcherImpl watcher = watcherMap.get(watchId);

        if (watcher == null) {
            LOG.warn("Response for unknown watch_id={}, available watchers: {}",
                watchId, watcherMap.keySet());
            return;
        }

        if (watcher.isClosed()) {
            LOG.debug("Response for closed watch_id={}", watchId);
            return;
        }

        try {
            LOG.debug("Routing response to watcher for watch_id={}", watchId);
            watcher.onNext(response);
        } catch (Exception e) {
            LOG.error("Error routing response for watch_id={}", watchId, e);
        }
    }

    private void onStreamEnded() {
        LOG.warn("Watch stream ended unexpectedly");
        if (refCount.get() > 0) {
            scheduleReconnect();
        }
    }

    private void onStreamError(Throwable error) {
        LOG.error("Watch stream error", error);
        if (refCount.get() > 0) {
            scheduleReconnect();
        }
    }

    private void handleConnectError(Throwable error) {
        LOG.error("Failed to connect watch stream", error);

        // Notify waiting watchers about the error
        notifyWaitingWatchers(error);

        if (refCount.get() > 0) {
            scheduleReconnect();
        }
    }

    private void notifyWaitingWatchers(Throwable error) {
        io.etcd.jetcd.common.exception.EtcdException etcdError =
            io.etcd.jetcd.common.exception.EtcdExceptionFactory.toEtcdException(error);

        watcherMap.values().forEach(watcher -> {
            if (!watcher.isClosed()) {
                try {
                    watcher.notifyError(etcdError);
                } catch (Exception e) {
                    LOG.error("Failed to notify watcher {} of connection error", watcher.getWatchId(), e);
                }
            }
        });
    }

    private void scheduleReconnect() {
        int attempt = reconnectAttempts.get();
        long delay = Math.min(INITIAL_RECONNECT_DELAY_MS * (1L << attempt), MAX_RECONNECT_DELAY_MS);

        LOG.debug("Scheduling reconnect attempt {} with delay {}ms", attempt + 1, delay);

        Futures.addCallback(executor.schedule(this::reconnect, delay, TimeUnit.MILLISECONDS), new FutureCallback<Object>() {
            @Override
            public void onFailure(Throwable t) {
                LOG.warn("Scheduled reconnect failed", t);
            }

            @Override
            public void onSuccess(Object result) {
            }
        }, executor);
    }

    private void notifyWatchersOfPermanentFailure() {
        io.etcd.jetcd.common.exception.EtcdException permanentError =
            io.etcd.jetcd.common.exception.EtcdExceptionFactory.newEtcdException(
                io.etcd.jetcd.common.exception.ErrorCode.UNAVAILABLE,
                "Watch stream failed to connect after " + MAX_RECONNECT_ATTEMPTS + " attempts");

        watcherMap.values().forEach(watcher -> {
            if (!watcher.isClosed()) {
                try {
                    watcher.notifyError(permanentError);
                } catch (Exception e) {
                    LOG.error("Failed to notify watcher {} of permanent failure", watcher.getWatchId(), e);
                }
            }
        });
    }

    private WatchGrpcClient createWatchClient() {
        io.etcd.jetcd.resolver.ServiceResolver serviceResolver = connectionManager.getServiceResolver();
        io.vertx.grpc.client.GrpcClient grpcClient = connectionManager.getAuthenticatedGrpcClient();
        io.vertx.core.net.SocketAddress targetAddress = (io.vertx.core.net.SocketAddress) serviceResolver.getTarget();

        return WatchGrpcClient.create(grpcClient, targetAddress);
    }
}

