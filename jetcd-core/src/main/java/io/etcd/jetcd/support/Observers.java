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

package io.etcd.jetcd.support;

import java.util.function.Consumer;

import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;

/**
 * Helper class for working with Vert.x ReadStream handlers.
 */
public final class Observers {
    private Observers() {
    }

    /**
     * Create a simple handler for ReadStream items.
     *
     * @param  onNext the handler for each item
     * @param  <V>    the item type
     * @return        a handler
     */
    public static <V> Handler<V> handler(Consumer<V> onNext) {
        return onNext::accept;
    }

    /**
     * Attach handlers to a ReadStream.
     *
     * @param stream      the ReadStream
     * @param onNext      handler for each item
     * @param onError     handler for errors
     * @param onCompleted handler for completion
     * @param <V>         the item type
     */
    public static <V> void observe(
        ReadStream<V> stream,
        Consumer<V> onNext,
        Consumer<Throwable> onError,
        Runnable onCompleted) {

        if (onNext != null) {
            stream.handler(onNext::accept);
        }
        if (onError != null) {
            stream.exceptionHandler(onError::accept);
        }
        if (onCompleted != null) {
            stream.endHandler(v -> onCompleted.run());
        }
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static final class Builder<V> {
        private Consumer<V> onNext;
        private Consumer<Throwable> onError;
        private Runnable onCompleted;

        public Builder<V> onNext(Consumer<V> onNext) {
            this.onNext = onNext;
            return this;
        }

        public Builder<V> onError(Consumer<Throwable> onError) {
            this.onError = onError;
            return this;
        }

        public Builder<V> onCompleted(Runnable onCompleted) {
            this.onCompleted = onCompleted;
            return this;
        }

        /**
         * Attach the built handlers to a ReadStream.
         *
         * @param  stream the ReadStream to attach handlers to
         * @return        the ReadStream (for chaining)
         */
        public ReadStream<V> attach(ReadStream<V> stream) {
            if (onNext != null) {
                stream.handler(onNext::accept);
            }
            if (onError != null) {
                stream.exceptionHandler(onError::accept);
            }
            if (onCompleted != null) {
                stream.endHandler(v -> onCompleted.run());
            }
            return stream;
        }
    }
}
