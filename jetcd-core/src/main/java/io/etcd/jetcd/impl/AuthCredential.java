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

import java.util.concurrent.CompletableFuture;

import io.etcd.jetcd.api.AuthGrpcClient;
import io.etcd.jetcd.api.AuthenticateRequest;

import com.google.protobuf.ByteString;

import static io.etcd.jetcd.support.Preconditions.checkArgument;

/**
 * Handles authentication token management for etcd requests.
 * In Vert.x, authentication is handled by adding the token as a header to each request.
 */
class AuthCredential {
    public static final String TOKEN_HEADER = "token";

    private final ClientConnectionManager manager;
    private volatile String token;

    public AuthCredential(ClientConnectionManager manager) {
        this.manager = manager;
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
    public void refresh() {
        token = null;
    }

    private CompletableFuture<String> authenticate() {
        checkArgument(!manager.builder().user().isEmpty(), "username can not be empty.");
        checkArgument(!manager.builder().password().isEmpty(), "password can not be empty.");

        io.etcd.jetcd.resolver.EndpointResolver endpointResolver = manager.getEndpointResolver();
        AuthGrpcClient authClient = AuthGrpcClient.create(
            manager.getGrpcClient(),
            (io.vertx.core.net.SocketAddress) endpointResolver.getTarget());

        final ByteString user = ByteString.copyFrom(this.manager.builder().user().getBytes());
        final ByteString pass = ByteString.copyFrom(this.manager.builder().password().getBytes());

        AuthenticateRequest request = AuthenticateRequest.newBuilder()
            .setNameBytes(user)
            .setPasswordBytes(pass)
            .build();

        return authClient.authenticate(request).toCompletionStage().toCompletableFuture().thenApply(response -> {
            this.token = response.getToken();
            return this.token;
        });
    }
}
