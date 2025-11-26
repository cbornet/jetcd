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

import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedRunnable;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.common.vertx.Failsafe;
import io.etcd.jetcd.resolver.ServiceResolver;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.net.endpoint.LoadBalancer;
import io.vertx.grpc.client.GrpcClient;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.toEtcdException;

final class ClientConnectionManager {
    private final Object lock;
    private final ClientBuilder builder;
    private final AuthCredential credential;
    private final Vertx vertx;
    private final boolean closeVertx;
    private volatile GrpcClient grpcClient;
    private volatile GrpcClient authenticatedGrpcClient;

    ClientConnectionManager(ClientBuilder builder) {
        this(builder, null);
    }

    ClientConnectionManager(ClientBuilder builder, GrpcClient grpcClient) {
        this.lock = new Object();
        this.builder = builder;
        this.grpcClient = grpcClient;
        this.credential = new AuthCredential(this);
        this.closeVertx = builder.vertx() == null;
        this.vertx = builder.vertx() != null
                ? builder.vertx()
                : Vertx.vertx(new VertxOptions().setUseDaemonThread(true));
    }

    GrpcClient getGrpcClient() {
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
    GrpcClient getAuthenticatedGrpcClient() {
        if (authenticatedGrpcClient == null) {
            synchronized (lock) {
                if (authenticatedGrpcClient == null) {
                    authenticatedGrpcClient = new AuthenticatingGrpcClient(getGrpcClient(), this);
                }
            }
        }

        return authenticatedGrpcClient;
    }

    ServiceResolver getServiceResolver() {
        if (builder.serviceResolver() == null) {
            throw new IllegalArgumentException("EndpointResolver must be configured");
        }
        return builder.serviceResolver();
    }

    ByteSequence getNamespace() {
        return builder.namespace();
    }

    ClientBuilder builder() {
        return builder;
    }

    AuthCredential authCredential() {
        return this.credential;
    }

    Vertx vertx() {
        return this.vertx;
    }

    void close() {
        synchronized (lock) {
            if (authenticatedGrpcClient != null) {
                authenticatedGrpcClient.close();
            }
            if (grpcClient != null) {
                grpcClient.close();
            }
            if (vertx != null && closeVertx) {
                // Only close Vertx if we created it ourselves
                // Use CompletableFuture to wait for completion to ensure proper cleanup
                try {
                    vertx.close().toCompletionStage().toCompletableFuture()
                        .get(5, java.util.concurrent.TimeUnit.SECONDS);
                } catch (Exception e) {
                    // Log but don't throw - best effort cleanup
                }
            }
        }
    }

    <R> CompletableFuture<R> withNewClient(
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

        // Configure load balancer (default to ROUND_ROBIN if not specified)
        LoadBalancer loadBalancer = builder.loadBalancer();
        if (loadBalancer == null) {
            loadBalancer = LoadBalancer.ROUND_ROBIN;
        }
        grpcBuilder.withLoadBalancer(loadBalancer);

        // Configure HTTP client options if provided (e.g., for SSL/TLS)
        HttpClientOptions httpClientOptions = builder.httpClientOptions();
        if (httpClientOptions != null) {
            grpcBuilder.with(httpClientOptions);
        }

        return (GrpcClient) grpcBuilder.build();
    }
}
