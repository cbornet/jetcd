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

package io.etcd.jetcd.grpc;

import java.util.concurrent.CompletableFuture;

import io.etcd.jetcd.resolver.ServiceResolver;
import io.etcd.jetcd.support.Util;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.Address;
import io.vertx.core.net.SocketAddress;
import io.vertx.grpc.client.GrpcClient;
import io.vertx.grpc.client.GrpcClientRequest;
import io.vertx.grpc.common.ServiceMethod;

import com.google.protobuf.ByteString;

import static io.etcd.jetcd.common.Preconditions.checkArgument;

/**
 * Manages authentication tokens for etcd requests.
 * Consolidates token management and GrpcClient wrapping with auth headers.
 */
public final class GrpcAuth {
    public static final String TOKEN_HEADER = "token";

    private final GrpcService grpcService;
    private volatile String token;

    GrpcAuth(GrpcService grpcService) {
        this.grpcService = grpcService;
    }

    /**
     * Get the current authentication token, authenticating if necessary.
     *
     * @return CompletableFuture with the token
     */
    public CompletableFuture<String> getToken() {
        final String currentToken = this.token;

        if (currentToken != null) {
            return CompletableFuture.completedFuture(currentToken);
        }

        return authenticate();
    }

    /**
     * Clear the cached token to force re-authentication on next request.
     */
    public void refreshToken() {
        token = null;
    }

    /**
     * Check if authentication is configured.
     *
     * @return true if user credentials are configured
     */
    public boolean requiresAuth() {
        return !Util.isNullOrEmpty(grpcService.builder().user());
    }

    /**
     * Wrap a GrpcClient to add authentication headers to all requests.
     *
     * @param  delegate the base GrpcClient to wrap
     * @return          a GrpcClient that adds auth headers
     */
    public GrpcClient wrapWithAuth(GrpcClient delegate) {
        return new AuthenticatingClient(delegate);
    }

    private CompletableFuture<String> authenticate() {
        checkArgument(!grpcService.builder().user().isEmpty(), "username can not be empty.");
        checkArgument(!grpcService.builder().password().isEmpty(), "password can not be empty.");

        ServiceResolver<?> serviceResolver = grpcService.getServiceResolver();

        io.etcd.jetcd.api.AuthGrpcClient authClient = io.etcd.jetcd.api.AuthGrpcClient.create(
            grpcService.getGrpcClient(),
            serviceResolver.getTarget(SocketAddress.class));

        final ByteString user = ByteString.copyFrom(this.grpcService.builder().user().getBytes());
        final ByteString pass = ByteString.copyFrom(this.grpcService.builder().password().getBytes());

        io.etcd.jetcd.api.AuthenticateRequest request = io.etcd.jetcd.api.AuthenticateRequest.newBuilder()
            .setNameBytes(user)
            .setPasswordBytes(pass)
            .build();

        return authClient.authenticate(request)
            .toCompletionStage()
            .toCompletableFuture()
            .thenApply(response -> {
                this.token = response.getToken();
                return this.token;
            });
    }

    /**
     * Inner class that wraps a GrpcClient to add authentication headers.
     */
    private final class AuthenticatingClient implements GrpcClient {

        private final GrpcClient delegate;

        AuthenticatingClient(GrpcClient delegate) {
            this.delegate = delegate;
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

        private <Req, Resp> Future<GrpcClientRequest<Req, Resp>> addHeaders(GrpcClientRequest<Req, Resp> req) {
            var builder = grpcService.builder();
            var headers = req.headers();

            builder.headers().forEach(headers::set);

            if (!requiresAuth()) {
                return Future.succeededFuture(req);
            }

            return Future.fromCompletionStage(getToken())
                .map(t -> {
                    headers.set(TOKEN_HEADER, t);
                    return req;
                });
        }
    }
}
