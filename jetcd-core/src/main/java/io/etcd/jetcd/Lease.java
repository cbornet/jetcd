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

package io.etcd.jetcd;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.lease.LeaseRevokeResponse;
import io.etcd.jetcd.lease.LeaseTimeToLiveResponse;
import io.etcd.jetcd.options.LeaseOption;
import io.etcd.jetcd.support.CloseableClient;

/**
 * Interface of KeepAlive talking to etcd.
 */
public interface Lease extends CloseableClient {

    /**
     * New a lease with ttl value.
     *
     * @param  ttl ttl value, unit seconds
     * @return     the grant response
     */
    CompletableFuture<LeaseGrantResponse> grant(long ttl);

    /**
     * New a lease with ttl value.Waits if necessary for at most the given time
     * if etcd server is available.
     *
     * @param  ttl     ttl value, unit seconds
     * @param  timeout the maximum time to waits
     * @param  unit    the time unit of the timeout argument
     * @return         The grant response
     */
    CompletableFuture<LeaseGrantResponse> grant(long ttl, long timeout, TimeUnit unit);

    /**
     * revoke one lease and the key bind to this lease will be removed.
     *
     * @param  leaseId id of the lease to revoke
     * @return         the revoke response
     */
    CompletableFuture<LeaseRevokeResponse> revoke(long leaseId);

    /**
     * keep alive one lease only once.
     *
     * @param  leaseId id of lease to keep alive once
     * @return         The keep alive response
     */
    CompletableFuture<LeaseKeepAliveResponse> keepAliveOnce(long leaseId);

    /**
     * retrieves the lease information of the given lease ID.
     *
     * @param  leaseId     id of lease
     * @param  leaseOption LeaseOption
     * @return             LeaseTimeToLiveResponse wrapped in CompletableFuture
     */
    CompletableFuture<LeaseTimeToLiveResponse> timeToLive(long leaseId, LeaseOption leaseOption);

    /**
     * keep the given lease alive forever.
     *
     * @param  leaseId  lease to be keep alive forever.
     * @param  listener the listener
     * @return          a CloseableClient that can be used to stop the keep alive.
     */
    CloseableClient keepAlive(long leaseId, Listener listener);

    /**
     * keep the given lease alive forever.
     *
     * @param  leaseId lease to be keep alive forever.
     * @param  onNext  the on next consumer
     * @return         a CloseableClient that can be used to stop the keep alive.
     */
    default CloseableClient keepAlive(long leaseId, Consumer<LeaseKeepAliveResponse> onNext) {
        return keepAlive(leaseId, listener(onNext));
    }

    /**
     * keep the given lease alive forever.
     *
     * @param  leaseId lease to be keep alive forever.
     * @param  onNext  the on next consumer
     * @param  onError the on error consumer
     * @return         a CloseableClient that can be used to stop the keep alive.
     */
    default CloseableClient keepAlive(long leaseId, Consumer<LeaseKeepAliveResponse> onNext, Consumer<Throwable> onError) {
        return keepAlive(leaseId, listener(onNext, onError));
    }

    /**
     * keep the given lease alive forever.
     *
     * @param  leaseId     lease to be keep alive forever.
     * @param  onNext      the on next consumer
     * @param  onError     the on error consumer
     * @param  onCompleted the on completion runnable
     * @return             a CloseableClient that can be used to stop the keep alive.
     */
    default CloseableClient keepAlive(long leaseId, Consumer<LeaseKeepAliveResponse> onNext, Consumer<Throwable> onError,
        Runnable onCompleted) {
        return keepAlive(leaseId, listener(onNext, onError, onCompleted));
    }

    static Listener listener(Consumer<LeaseKeepAliveResponse> onNext) {
        return listener(onNext, t -> {
        }, () -> {
        });
    }

    static Listener listener(Consumer<LeaseKeepAliveResponse> onNext, Consumer<Throwable> onError) {
        return listener(onNext, onError, () -> {
        });
    }

    static Listener listener(Consumer<LeaseKeepAliveResponse> onNext, Consumer<Throwable> onError, Runnable onCompleted) {
        return new Listener() {
            @Override
            public void onNext(LeaseKeepAliveResponse response) {
                onNext.accept(response);
            }

            @Override
            public void onError(Throwable throwable) {
                onError.accept(throwable);
            }

            @Override
            public void onCompleted() {
                onCompleted.run();
            }
        };
    }

    /**
     * Listener for lease keep alive responses.
     */
    interface Listener {
        /**
         * Invoked on new keep alive responses.
         *
         * @param response the response.
         */
        void onNext(LeaseKeepAliveResponse response);

        /**
         * Invoked on errors.
         *
         * @param throwable the error.
         */
        void onError(Throwable throwable);

        /**
         * Invoked on completion.
         */
        void onCompleted();
    }
}
