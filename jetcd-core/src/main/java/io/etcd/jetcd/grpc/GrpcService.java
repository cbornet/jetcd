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
import io.etcd.jetcd.common.suppliers.AsyncCloseable;
import io.etcd.jetcd.common.suppliers.AsyncCloseableSupplier;
import io.etcd.jetcd.common.suppliers.Suppliers;
import io.etcd.jetcd.resolver.ServiceResolver;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.net.endpoint.LoadBalancer;
import io.vertx.grpc.client.GrpcClient;

import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.toEtcdException;

/**
 * Manages gRPC client services and lifecycle for etcd operations.
 * Provides access to base and authenticated gRPC clients with lazy initialization.
 */
public final class GrpcService {
    private final ClientBuilder builder;
    private final GrpcAuth auth;
    private final Vertx vertx;
    private final boolean closeVertx;
    private final AsyncCloseableSupplier<GrpcClientHolder> grpcClientSupplier;
    private final AsyncCloseableSupplier<GrpcClientHolder> authenticatedGrpcClientSupplier;

    /**
     * Creates a new GrpcService with the given builder.
     *
     * @param builder the client builder
     */
    public GrpcService(ClientBuilder builder) {
        this.builder = builder;
        this.closeVertx = builder.vertx() == null;
        this.vertx = builder.vertx() != null
            ? builder.vertx()
            : Vertx.vertx(new VertxOptions().setUseDaemonThread(true));
        this.auth = new GrpcAuth(this);

        // Use memoizing suppliers for thread-safe lazy initialization
        this.grpcClientSupplier = Suppliers.memoizingAsyncCloseable(
            () -> new GrpcClientHolder(createGrpcClient()));
        this.authenticatedGrpcClientSupplier = Suppliers.memoizingAsyncCloseable(
            () -> new GrpcClientHolder(auth.wrapWithAuth(getGrpcClient())));
    }

    /**
     * Returns the gRPC client for this service.
     *
     * @return the gRPC client
     */
    public GrpcClient getGrpcClient() {
        return grpcClientSupplier.get().client();
    }

    /**
     * Get the authenticated GrpcClient that adds auth token headers to all requests.
     * Use this for all authenticated operations (KV, Watch, Lease, etc.).
     *
     * @return the authenticated GrpcClient
     */
    public GrpcClient getAuthenticatedGrpcClient() {
        return authenticatedGrpcClientSupplier.get().client();
    }

    /**
     * Returns the service resolver for this service.
     *
     * @return the service resolver
     */
    public ServiceResolver<SocketAddress> getServiceResolver() {
        if (builder.serviceResolver() == null) {
            throw new IllegalArgumentException("EndpointResolver must be configured");
        }
        return builder.serviceResolver();
    }

    /**
     * Returns the namespace for this service.
     *
     * @return the namespace
     */
    public ByteSequence getNamespace() {
        return builder.namespace();
    }

    /**
     * Returns the client builder for this service.
     *
     * @return the client builder
     */
    public ClientBuilder builder() {
        return builder;
    }

    /**
     * Returns the auth service.
     *
     * @return the GrpcAuth instance
     */
    public GrpcAuth auth() {
        return this.auth;
    }

    /**
     * Returns the Vert.x instance used by this service.
     *
     * @return the Vert.x instance
     */
    public Vertx vertx() {
        return this.vertx;
    }

    /**
     * Closes this service and releases all resources.
     *
     * @return a CompletableFuture that completes when the service is closed
     */
    public CompletableFuture<Void> close() {
        // Close clients using the supplier's async close
        return authenticatedGrpcClientSupplier.close()
            .compose(v -> grpcClientSupplier.close())
            .compose(v -> {
                if (auth != null) {
                    auth.close();
                }
                return Future.succeededFuture();
            })
            .compose(v -> {
                if (vertx != null && closeVertx) {
                    return vertx.close();
                }
                return Future.succeededFuture();
            })
            .toCompletionStage()
            .toCompletableFuture();
    }

    /**
     * Creates a new temporary client for the given target and executes the consumer.
     *
     * @param  target         the target endpoint
     * @param  clientConsumer the consumer function
     * @param  <R>            the result type
     * @return                a CompletableFuture with the result
     */
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

    /**
     * Wrapper that adapts GrpcClient to AsyncCloseable for use with memoizing suppliers.
     */
    private static final class GrpcClientHolder implements AsyncCloseable {
        private final GrpcClient client;

        GrpcClientHolder(GrpcClient client) {
            this.client = client;
        }

        GrpcClient client() {
            return client;
        }

        @Override
        public Future<Void> close() {
            return client.close();
        }
    }
}
