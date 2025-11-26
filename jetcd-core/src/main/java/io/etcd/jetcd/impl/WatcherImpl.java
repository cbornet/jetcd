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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.api.WatchCreateRequest;
import io.etcd.jetcd.api.WatchResponse;
import io.etcd.jetcd.common.exception.EtcdException;
import io.etcd.jetcd.options.OptionsUtil;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.support.Util;
import io.vertx.grpc.common.GrpcStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.etcd.jetcd.common.exception.ErrorCode.FAILED_PRECONDITION;
import static io.etcd.jetcd.common.exception.ErrorCode.INTERNAL;
import static io.etcd.jetcd.common.exception.ErrorCode.OUT_OF_RANGE;
import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.newCompactedException;
import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.newEtcdException;
import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.toEtcdException;

/**
 * Individual watcher that delegates stream operations to shared WatchStreamManager.
 */
public final class WatcherImpl implements Watch.Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(WatcherImpl.class);

    private final WatchStream stream;
    private final long watchId;
    private final ByteSequence key;
    private final ByteSequence namespace;
    private final WatchOption option;
    private final Watch.Listener listener;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<CountDownLatch> createdLatch;
    private final Object parentLock;
    private final AtomicBoolean parentClosed;
    private final ClientConnectionManager connectionManager;
    private final ListeningScheduledExecutorService executor;
    private final Runnable onClose;
    private long revision;

    public WatcherImpl(
            WatchStream stream,
            long watchId,
            ByteSequence key,
            ByteSequence namespace,
            WatchOption option,
            Watch.Listener listener,
            Object parentLock,
            AtomicBoolean parentClosed,
            ClientConnectionManager connectionManager,
            ListeningScheduledExecutorService executor,
            Runnable onClose) {
        this.stream = stream;
        this.watchId = watchId;
        this.key = key;
        this.namespace = namespace;
        this.option = option;
        this.listener = listener;
        this.createdLatch = new AtomicReference<>(new CountDownLatch(1));
        this.revision = option.getRevision();
        this.parentLock = parentLock;
        this.parentClosed = parentClosed;
        this.connectionManager = connectionManager;
        this.executor = executor;
        this.onClose = onClose;
    }

    public long getWatchId() {
        return watchId;
    }

    @Override
    public boolean isClosed() {
        return this.closed.get() || this.parentClosed.get();
    }

    public CountDownLatch getCreatedLatch() {
        return createdLatch.get();
    }

    public void resume() {
        if (isClosed()) {
            LOG.debug("Watcher {} is closed, skipping resume", watchId);
            return;
        }

        createdLatch.set(new CountDownLatch(1));

        WatchCreateRequest.Builder builder = WatchCreateRequest.newBuilder()
            .setWatchId(watchId)
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

        WatchCreateRequest request = builder.build();

        // Delegate to stream interface
        stream.sendCreateRequest(request);
    }

    @Override
    public void close() {
        synchronized (parentLock) {
            if (closed.compareAndSet(false, true)) {
                // Notify listener
                listener.onCompleted();

                // Notify parent (WatchImpl) to remove from list and call streamManager.onWatcherClosed
                onClose.run();
            }
        }
    }

    @Override
    public void requestProgress() {
        if (closed.get()) {
            LOG.warn("WatcherImpl.requestProgress: watcher is closed, key={}", key);
            return;
        }

        try {
            CountDownLatch latch = createdLatch.get();
            if (latch != null && !latch.await(5, TimeUnit.SECONDS)) {
                LOG.warn("Timeout waiting for watch creation");
                return;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        stream.sendProgressRequest();
    }

    public void notifyError(EtcdException error) {
        synchronized (parentLock) {
            if (!isClosed()) {
                listener.onError(error);
            }
        }
    }

    public void onNext(WatchResponse response) {
        if (closed.get()) {
            return;
        }

        // Handle special case: created and canceled simultaneously (auth error)
        if (response.getCreated() && response.getCanceled()
            && !response.getCancelReason().isEmpty()
            && (response.getCancelReason().contains("etcdserver: permission denied") ||
                response.getCancelReason().contains("etcdserver: invalid auth token"))) {

            connectionManager.authCredential().refresh();
            GrpcStatus error = GrpcStatus.CANCELLED;
            handleError(toEtcdException(error), true);
        } else if (response.getCreated()) {
            // Watch created
            if (response.getWatchId() == -1) {
                listener.onError(newEtcdException(INTERNAL, "etcd server failed to create watch id"));
                return;
            }

            revision = Math.max(revision, response.getHeader().getRevision());

            CountDownLatch latch = createdLatch.get();
            if (latch != null) {
                latch.countDown();
            }

            if (option.isCreatedNotify()) {
                listener.onNext(new io.etcd.jetcd.watch.WatchResponse(response));
            }
        } else if (response.getCanceled()) {
            // Watch canceled
            String reason = response.getCancelReason();
            Throwable error;

            if (response.getCompactRevision() != 0) {
                error = newCompactedException(response.getCompactRevision());
            } else if (Strings.isNullOrEmpty(reason)) {
                error = newEtcdException(OUT_OF_RANGE,
                    "etcdserver: mvcc: required revision is a future revision");
            } else {
                error = newEtcdException(FAILED_PRECONDITION, reason);
            }

            handleError(toEtcdException(error), false);
        } else if (io.etcd.jetcd.watch.WatchResponse.isProgressNotify(response)) {
            listener.onNext(new io.etcd.jetcd.watch.WatchResponse(response));
            revision = Math.max(revision, response.getHeader().getRevision());
        } else if (response.getEventsCount() == 0 && option.isProgressNotify()) {
            // Progress notify
            listener.onNext(new io.etcd.jetcd.watch.WatchResponse(response, namespace));
            revision = response.getHeader().getRevision();
        } else if (response.getEventsCount() > 0) {
            // Events
            listener.onNext(new io.etcd.jetcd.watch.WatchResponse(response, namespace));
            revision = response.getEvents(response.getEventsCount() - 1).getKv().getModRevision() + 1;
        }
    }

    private void handleError(EtcdException etcdException, boolean shouldReschedule) {
        synchronized (parentLock) {
            if (isClosed()) {
                return;
            }

            listener.onError(etcdException);
        }

        if (shouldReschedule) {
            if (etcdException.getMessage().contains("etcdserver: permission denied")) {
                connectionManager.authCredential().refresh();
            }

            reschedule();
            return;
        }

        close();
    }

    private void reschedule() {
        Futures.addCallback(executor.schedule(this::resume, 500, TimeUnit.MILLISECONDS), new FutureCallback<Object>() {
            @Override
            public void onFailure(Throwable t) {
                LOG.warn("scheduled resume failed for watch_id={}", watchId, t);
            }

            @Override
            public void onSuccess(Object result) {
            }
        }, executor);
    }
}

