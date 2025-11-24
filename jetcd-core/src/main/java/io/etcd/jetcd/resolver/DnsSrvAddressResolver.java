/*
 * Copyright 2016-2025 The jetcd authors
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

package io.etcd.jetcd.resolver;

import java.util.List;
import java.util.stream.Collectors;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.dns.DnsClient;
import io.vertx.core.dns.DnsClientOptions;
import io.vertx.core.dns.SrvRecord;
import io.vertx.core.net.AddressResolver;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.endpoint.EndpointResolver;

/**
 * AddressResolver that uses Vert.x DnsClient to resolve DNS SRV records
 * and converts them directly to SocketAddress instances compatible with gRPC client stubs.
 * <p>
 * The DnsClient is initialized lazily when the Vertx instance becomes available.
 */
public final class DnsSrvAddressResolver implements AddressResolver<SocketAddress> {
    private final String serviceName;
    private final DnsClientOptions dnsOptions;
    private volatile DnsClient dnsClient;

    public DnsSrvAddressResolver(String serviceName, DnsClientOptions dnsOptions) {
        this.serviceName = serviceName;
        this.dnsOptions = dnsOptions;
    }

    @Override
    public EndpointResolver<SocketAddress, ?, ?, ?> endpointResolver(Vertx vertx) {
        // Initialize DnsClient lazily when Vertx instance is provided
        if (dnsClient == null) {
            synchronized (this) {
                if (dnsClient == null) {
                    dnsClient = vertx.createDnsClient(dnsOptions);
                }
            }
        }

        final DnsClient client = dnsClient;

        // Use the mappingResolver pattern to convert DNS SRV lookups to socket addresses
        AddressResolver<SocketAddress> resolver = AddressResolver.mappingResolver(sockAddr -> {
            try {
                // Query DNS SRV records synchronously (blocking on the async Future)
                Future<List<SrvRecord>> srvFuture = client.resolveSRV(serviceName);
                List<SrvRecord> srvRecords = srvFuture.toCompletionStage().toCompletableFuture().get();

                if (srvRecords.isEmpty()) {
                    throw new IllegalStateException("No SRV records found for " + serviceName);
                }

                // Convert SrvRecord list to SocketAddress list
                return srvRecords.stream()
                    .map(srv -> SocketAddress.inetSocketAddress(srv.port(), srv.target()))
                    .collect(Collectors.toList());
            } catch (Exception e) {
                throw new RuntimeException("Failed to resolve DNS SRV records for " + serviceName, e);
            }
        });

        // Delegate to the resolver's endpointResolver method
        return resolver.endpointResolver(vertx);
    }
}

