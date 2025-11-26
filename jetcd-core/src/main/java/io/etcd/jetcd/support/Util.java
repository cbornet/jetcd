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

import com.google.protobuf.ByteString;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Constants;
import io.vertx.core.Future;
import io.vertx.core.net.SocketAddress;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

public final class Util {

    private Util() {
    }

    public static List<URI> toURIs(Collection<String> uris) {
        return uris.stream().map(uri -> {
            try {
                return new URI(uri);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid endpoint URI: " + uri, e);
            }
        }).collect(Collectors.toList());
    }

    /**
     * Converts a URI to a SocketAddress, using default port 2379 if not specified.
     *
     * @param  uri the URI to convert
     * @return     a SocketAddress for the URI
     */
    public static SocketAddress toSocketAddress(URI uri) {
        String host = uri.getHost();
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("URI host cannot be null or empty: " + uri);
        }
        int port = uri.getPort() != -1 ? uri.getPort() : 2379;
        return SocketAddress.inetSocketAddress(port, host);
    }

    /**
     * Checks if a ByteSequence is null or empty.
     *
     * @param  sequence the ByteSequence to check
     * @return          true if the sequence is null or empty, false otherwise
     */
    public static boolean isNullOrEmpty(ByteSequence sequence) {
        return sequence == null || sequence.isEmpty();
    }

    public static ByteString prefixNamespace(ByteSequence key, ByteSequence namespace) {
        return ByteString.copyFrom(namespace.isEmpty() ? key.getBytes() : namespace.concat(key).getBytes());
    }

    public static ByteString prefixNamespace(ByteString key, ByteSequence namespace) {
        return namespace.isEmpty() ? key : ByteString.copyFrom(namespace.concat(key).getBytes());
    }

    public static ByteString prefixNamespaceToRangeEnd(ByteSequence end, ByteSequence namespace) {
        if (namespace.isEmpty()) {
            return ByteString.copyFrom(end.getBytes());
        }

        if (end.size() == 1 && end.getBytes()[0] == 0) {
            // range end is '\0', calculate the prefixed range end by (key + 1)
            byte[] prefixedEndArray = namespace.getBytes();
            boolean ok = false;
            for (int i = prefixedEndArray.length - 1; i >= 0; i--) {
                prefixedEndArray[i] = (byte) (prefixedEndArray[i] + 1);
                if (prefixedEndArray[i] != 0) {
                    ok = true;
                    break;
                }
            }
            if (!ok) {
                // 0xff..ff => 0x00
                prefixedEndArray = Constants.NULL_KEY.getBytes();
            }
            return ByteString.copyFrom(prefixedEndArray);
        } else {
            return ByteString.copyFrom(namespace.concat(end).getBytes());
        }
    }

    public static ByteString prefixNamespaceToRangeEnd(ByteString end, ByteSequence namespace) {
        if (namespace.isEmpty()) {
            return end;
        }

        if (end.size() == 1 && end.toByteArray()[0] == 0) {
            // range end is '\0', calculate the prefixed range end by (key + 1)
            byte[] prefixedEndArray = namespace.getBytes();
            boolean ok = false;
            for (int i = prefixedEndArray.length - 1; i >= 0; i--) {
                prefixedEndArray[i] = (byte) (prefixedEndArray[i] + 1);
                if (prefixedEndArray[i] != 0) {
                    ok = true;
                    break;
                }
            }
            if (!ok) {
                // 0xff..ff => 0x00
                prefixedEndArray = Constants.NULL_KEY.getBytes();
            }
            return ByteString.copyFrom(prefixedEndArray);
        } else {
            return ByteString.copyFrom(namespace.concat(end).getBytes());
        }
    }

    public static ByteString unprefixNamespace(ByteString key, ByteSequence namespace) {
        return namespace.isEmpty() ? key : key.substring(namespace.size());
    }

    /**
     * Convert a Vert.x Future to a CompletableFuture.
     *
     * @param      vertxFuture the Vert.x future
     * @param      <T>         the result type
     * @return                 a CompletableFuture
     * @deprecated             Use {@link Future#toCompletionStage()} and
     *                         {@link java.util.concurrent.CompletionStage#toCompletableFuture()} instead
     */
    @Deprecated
    @SuppressWarnings("InlineMeSuggester")
    public static <T> CompletableFuture<T> toCompletableFuture(Future<T> vertxFuture) {
        return vertxFuture.toCompletionStage().toCompletableFuture();
    }

    public static ThreadFactory createThreadFactory(String prefix, boolean daemon) {
        ThreadFactory backingThreadFactory = Executors.defaultThreadFactory();

        return r -> {
            Thread t = backingThreadFactory.newThread(r);
            t.setDaemon(daemon);
            // set a proper name so it is easier to find out the where the thread was created
            t.setName(prefix + t.getName());
            return t;
        };
    }

}
