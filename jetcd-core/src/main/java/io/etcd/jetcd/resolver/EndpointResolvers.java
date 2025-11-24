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

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.etcd.jetcd.support.Util;
import io.vertx.core.Vertx;
import io.vertx.core.dns.DnsClientOptions;
import io.vertx.core.net.Address;
import io.vertx.core.net.AddressResolver;
import io.vertx.core.net.SocketAddress;

/**
 * Utility class providing factory methods for creating endpoint resolvers.
 *
 * <p>
 * This class offers convenient ways to create endpoint resolvers for various service discovery mechanisms:
 * </p>
 * <ul>
 * <li>Static endpoint lists via {@link #endpoints(String...)}</li>
 * <li>DNS SRV-based discovery via {@link #dnsSrv(String)}</li>
 * </ul>
 */
public final class EndpointResolvers {

    private EndpointResolvers() {
        // Utility class, no instantiation
    }

    /**
     * Creates an endpoint resolver for static etcd endpoints.
     *
     * @param  addresses etcd server addresses (e.g., "http://localhost:2379")
     * @return           an endpoint resolver for the specified addresses
     */
    public static EndpointResolver endpoints(String... addresses) {
        List<URI> uris = Stream.of(addresses)
            .map(URI::create)
            .collect(Collectors.toList());
        return Static.fromEndpoints(uris);
    }

    /**
     * Creates an endpoint resolver for static etcd endpoints.
     *
     * @param  endpoints etcd server endpoint URIs
     * @return           an endpoint resolver for the specified endpoints
     */
    public static EndpointResolver endpoints(URI... endpoints) {
        return Static.fromEndpoints(Arrays.asList(endpoints));
    }

    /**
     * Creates an endpoint resolver that uses DNS SRV records for service discovery.
     *
     * @param  serviceName the DNS SRV service name (e.g., "_etcd._tcp.example.com")
     * @return             an endpoint resolver that uses DNS SRV records
     */
    public static EndpointResolver dnsSrv(String serviceName) {
        return DnsSrv.create(serviceName);
    }

    /**
     * Creates an endpoint resolver that uses DNS SRV records with a custom DNS server.
     *
     * @param  serviceName the DNS SRV service name (e.g., "_etcd._tcp.example.com")
     * @param  dnsServer   the DNS server hostname
     * @param  dnsPort     the DNS server port (usually 53)
     * @return             an endpoint resolver that uses DNS SRV records
     */
    public static EndpointResolver dnsSrv(String serviceName, String dnsServer, int dnsPort) {
        return DnsSrv.create(serviceName, dnsServer, dnsPort);
    }

    /**
     * Static endpoint resolver implementation.
     *
     * <p>
     * Resolves to a fixed list of etcd server endpoints. Use this when you have a known,
     * unchanging list of etcd servers.
     * </p>
     */
    public static final class Static extends AbstractEndpointResolver {

        /**
         * Creates a static endpoint resolver from a list of endpoint URIs.
         *
         * @param  endpoints list of etcd endpoint URIs
         * @return           a static endpoint resolver
         */
        public static Static fromEndpoints(List<URI> endpoints) {
            List<SocketAddress> addresses = endpoints.stream()
                .map(Util::toSocketAddress)
                .collect(Collectors.toList());

            AddressResolver resolver = AddressResolver.mappingResolver(ignored -> addresses);
            Address target = SocketAddress.inetSocketAddress(2379, "etcd-cluster");

            return new Static(resolver, target);
        }

        private Static(AddressResolver resolver, Address target) {
            super(resolver, target);
        }
    }

    /**
     * DNS SRV endpoint resolver implementation.
     *
     * <p>
     * Uses DNS SRV records to discover etcd endpoints dynamically. This is useful in
     * cloud environments where server addresses may change. Requires the
     * vertx-service-resolver dependency.
     * </p>
     *
     * <p>
     * DNS SRV records allow service discovery through DNS. For example,
     * "_etcd._tcp.example.com" would resolve to multiple etcd servers with
     * priority and weight information.
     * </p>
     */
    public static final class DnsSrv extends AbstractEndpointResolver {

        /**
         * Creates a DNS SRV resolver with default DNS server.
         *
         * @param  serviceName the DNS SRV service name (e.g., "_etcd._tcp.example.com")
         * @return             an endpoint resolver that uses DNS SRV records
         */
        public static DnsSrv create(String serviceName) {
            DnsClientOptions dnsOptions = new DnsClientOptions();
            DnsSrvAddressResolver resolver = new DnsSrvAddressResolver(serviceName, dnsOptions);
            // Use a placeholder SocketAddress that will be resolved by our custom resolver
            Address target = SocketAddress.inetSocketAddress(2379, serviceName);

            return new DnsSrv(resolver, target);
        }

        /**
         * Creates a DNS SRV resolver with a specific DNS server.
         *
         * @param  serviceName the DNS SRV service name (e.g., "_etcd._tcp.example.com")
         * @param  dnsServer   the DNS server hostname
         * @param  dnsPort     the DNS server port (usually 53)
         * @return             an endpoint resolver that uses DNS SRV records
         */
        public static DnsSrv create(String serviceName, String dnsServer, int dnsPort) {
            DnsClientOptions dnsOptions = new DnsClientOptions()
                .setHost(dnsServer)
                .setPort(dnsPort);
            DnsSrvAddressResolver resolver = new DnsSrvAddressResolver(serviceName, dnsOptions);
            // Use a placeholder SocketAddress that will be resolved by our custom resolver
            Address target = SocketAddress.inetSocketAddress(2379, serviceName);

            return new DnsSrv(resolver, target);
        }

        private DnsSrv(AddressResolver resolver, Address target) {
            super(resolver, target);
        }
    }
}
