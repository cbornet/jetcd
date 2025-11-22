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

import io.etcd.jetcd.support.Util;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.Address;
import io.vertx.grpc.client.GrpcClient;
import io.vertx.grpc.client.GrpcClientRequest;
import io.vertx.grpc.common.ServiceMethod;

/**
 * A GrpcClient wrapper that automatically adds authentication token headers to all requests.
 * This allows transparent authentication without modifying individual service implementations.
 */
final class AuthenticatingGrpcClient implements GrpcClient {

    private final GrpcClient delegate;
    private final ClientConnectionManager manager;

    AuthenticatingGrpcClient(GrpcClient delegate, ClientConnectionManager manager) {
        this.delegate = delegate;
        this.manager = manager;
    }

    @Override
    public <Req, Resp> Future<GrpcClientRequest<Req, Resp>> request(Address address, ServiceMethod<Resp, Req> method) {
        return delegate.request(address, method).compose(this::addHeaders);
    }

    @Override
    public <Req, Resp> Future<GrpcClientRequest<Req, Resp>> request(ServiceMethod<Resp, Req> method) {
        return delegate.request(method).compose(this::addHeaders);
    }

    @Override
    public Future<GrpcClientRequest<Buffer, Buffer>> request(Address address) {
        return delegate.request(address);
    }

    @Override
    public Future<GrpcClientRequest<Buffer, Buffer>> request() {
        return delegate.request();
    }

    @Override
    public Future<Void> close() {
        return delegate.close();
    }

    /**
     * Add custom headers and authentication token to the request.
     * Custom headers are added first, then the authentication token if credentials are configured.
     */
    private <Req, Resp> Future<GrpcClientRequest<Req, Resp>> addHeaders(GrpcClientRequest<Req, Resp> req) {
        var builder = manager.builder();
        var headers = req.headers();

        // Add custom headers from builder configuration
        builder.headers().forEach(headers::set);

        // Skip auth token if no auth configured
        if (Util.isNullOrEmpty(builder.user())) {
            return Future.succeededFuture(req);
        }

        // Get token and add to headers
        return Future.fromCompletionStage(manager.authCredential().getToken())
            .map(token -> {
                headers.set(AuthCredential.TOKEN_HEADER, token);
                return req;
            });
    }
}
