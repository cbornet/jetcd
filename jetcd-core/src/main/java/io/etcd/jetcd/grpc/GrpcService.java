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
import java.util.function.Function;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.resolver.ServiceResolver;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.net.endpoint.LoadBalancer;
import io.vertx.grpc.client.GrpcClient;

import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.toEtcdException;

/**
 * Manages gRPC client services and lifecycle for etcd operations.
 * Provides access to base and authenticated gRPC clients with lazy initialization.
 */
public final class GrpcService {
    private final Object lock;
    private final ClientBuilder builder;
    private final GrpcAuth auth;
    private final Vertx vertx;
    private final boolean closeVertx;
    private volatile GrpcClient grpcClient;
    private volatile GrpcClient authenticatedGrpcClient;

    public GrpcService(ClientBuilder builder) {
        this(builder, null);
    }

    public GrpcService(ClientBuilder builder, GrpcClient grpcClient) {
        this.lock = new Object();
        this.builder = builder;
        this.grpcClient = grpcClient;
        this.auth = new GrpcAuth(this);
        this.closeVertx = builder.vertx() == null;
        this.vertx = builder.vertx() != null
            ? builder.vertx()
            : Vertx.vertx(new VertxOptions().setUseDaemonThread(true));
    }

    public GrpcClient getGrpcClient() {
        if (grpcClient == null) {
            synchronized (lock) {
                if (grpcClient == null) {
                    grpcClient = createGrpcClient();
                }
            }
        }

        return grpcClient;
    }

    /**
     * Get the authenticated GrpcClient that adds auth token headers to all requests.
     * Use this for all authenticated operations (KV, Watch, Lease, etc.).
     *
     * @return the authenticated GrpcClient
     */
    public GrpcClient getAuthenticatedGrpcClient() {
        if (authenticatedGrpcClient == null) {
            synchronized (lock) {
                if (authenticatedGrpcClient == null) {
                    authenticatedGrpcClient = auth.wrapWithAuth(getGrpcClient());
                }
            }
        }

        return authenticatedGrpcClient;
    }

    public ServiceResolver<?> getServiceResolver() {
        if (builder.serviceResolver() == null) {
            throw new IllegalArgumentException("EndpointResolver must be configured");
        }
        return builder.serviceResolver();
    }

    public ByteSequence getNamespace() {
        return builder.namespace();
    }

    public ClientBuilder builder() {
        return builder;
    }

    public GrpcAuth auth() {
        return this.auth;
    }

    public Vertx vertx() {
        return this.vertx;
    }

    public CompletableFuture<Void> close() {
        return CompletableFuture.runAsync(() -> {
            synchronized (lock) {
                if (authenticatedGrpcClient != null) {
                    authenticatedGrpcClient.close();
                }
                if (grpcClient != null) {
                    grpcClient.close();
                }
                if (auth != null) {
                    auth.close();
                }
            }
        }).thenCompose(v -> {
            if (vertx != null && closeVertx) {
                return vertx.close().toCompletionStage().toCompletableFuture();
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    public <R> CompletableFuture<R> withNewClient(
        String target,
        Function<GrpcClient, CompletableFuture<R>> clientConsumer) {

        final GrpcClient client = GrpcClient.client(this.vertx);

        try {
            return clientConsumer.apply(client).whenComplete((r, t) -> client.close());
        } catch (Exception e) {
            client.close();
            throw toEtcdException(e);
        }
    }

    private GrpcClient createGrpcClient() {
        io.vertx.grpc.client.GrpcClientBuilder<?> grpcBuilder = GrpcClient.builder(this.vertx);

        ServiceResolver<?> serviceResolver = getServiceResolver();
        grpcBuilder.withAddressResolver(serviceResolver.getResolver());

        LoadBalancer loadBalancer = builder.loadBalancer();
        if (loadBalancer == null) {
            loadBalancer = LoadBalancer.ROUND_ROBIN;
        }
        grpcBuilder.withLoadBalancer(loadBalancer);

        HttpClientOptions httpClientOptions = builder.httpClientOptions();
        if (httpClientOptions != null) {
            grpcBuilder.with(httpClientOptions);
        }

        return (GrpcClient) grpcBuilder.build();
    }
}
