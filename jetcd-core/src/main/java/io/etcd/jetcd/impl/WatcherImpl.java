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
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.api.WatchCreateRequest;
import io.etcd.jetcd.common.exception.EtcdException;
import io.etcd.jetcd.options.OptionsUtil;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.support.Util;
import io.etcd.jetcd.watch.WatchResponse;
import io.vertx.core.Vertx;
import io.vertx.grpc.common.GrpcStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

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
    private final Object lock;
    private final AtomicBoolean parentClosed;
    private final ClientConnectionManager connectionManager;
    private final Vertx vertx;
    private final Runnable onClose;
    private long revision;
    private volatile Long pendingTimerId;

    public WatcherImpl(
        WatchStream stream,
        long watchId,
        ByteSequence key,
        ByteSequence namespace,
        WatchOption option,
        Watch.Listener listener,
        Object lock,
        AtomicBoolean parentClosed,
        ClientConnectionManager connectionManager,
        Runnable onClose) {
        this.stream = stream;
        this.watchId = watchId;
        this.key = key;
        this.namespace = namespace;
        this.option = option;
        this.listener = listener;
        this.revision = option.getRevision();
        this.lock = lock;
        this.parentClosed = parentClosed;
        this.connectionManager = connectionManager;
        this.vertx = connectionManager.vertx();
        this.onClose = onClose;
    }

    public long getWatchId() {
        return watchId;
    }

    @Override
    public boolean isClosed() {
        return this.closed.get() || this.parentClosed.get();
    }

    public void resume() {
        if (isClosed()) {
            LOG.debug("Watcher {} is closed, skipping resume", watchId);
            return;
        }

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
        synchronized (lock) {
            if (closed.compareAndSet(false, true)) {
                // Cancel pending timer
                if (pendingTimerId != null) {
                    vertx.cancelTimer(pendingTimerId);
                    pendingTimerId = null;
                }

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

        stream.sendProgressRequest();
    }

    public void notifyError(EtcdException error) {
        synchronized (lock) {
            if (!isClosed()) {
                listener.onError(error);
            }
        }
    }

    public void onNext(io.etcd.jetcd.api.WatchResponse response) {
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

        if (handleEvents(response)) {
            return;
        }

        LOG.debug("Received WatchResponse with no matching scenario: {}", response);
    }

    private void updateRevision(long newRevision) {
        revision = Math.max(revision, newRevision);
    }

    private void updateRevisionFromEvents(io.etcd.jetcd.api.WatchResponse response) {
        if (response.getEventsCount() > 0) {
            long eventRevision = response.getEvents(response.getEventsCount() - 1)
                .getKv()
                .getModRevision() + 1;
            revision = eventRevision;
        }
    }

    private void notifyListener(io.etcd.jetcd.api.WatchResponse response, boolean withNamespace) {
        WatchResponse watchResponse = withNamespace
            ? new WatchResponse(response, namespace)
            : new WatchResponse(response);

        listener.onNext(watchResponse);
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
            GrpcStatus error = GrpcStatus.CANCELLED;
            handleError(toEtcdException(error), true);
            return true;
        }

        return false;
    }

    private boolean handleWatchCreated(io.etcd.jetcd.api.WatchResponse response) {
        if (!response.getCreated()) {
            return false;
        }

        if (response.getWatchId() == -1) {
            listener.onError(newEtcdException(INTERNAL, "etcd server failed to create watch id"));
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
        synchronized (lock) {
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
        synchronized (lock) {
            if (isClosed()) {
                return;
            }

            pendingTimerId = vertx.setTimer(500, timerId -> {
                synchronized (lock) {
                    pendingTimerId = null;
                    if (isClosed()) {
                        return;
                    }
                }

                try {
                    resume();
                } catch (Exception e) {
                    LOG.warn("scheduled resume failed for watch_id={}", watchId, e);
                }
            });
        }
    }
}
