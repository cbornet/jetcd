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

package io.etcd.jetcd.test;

import java.util.List;
import java.util.stream.Collectors;

import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.launcher.EtcdContainer;
import io.etcd.jetcd.resolver.EndpointResolver;
import io.vertx.core.net.Address;
import io.vertx.core.net.AddressResolver;
import io.vertx.core.net.SocketAddress;

/**
 * An endpoint resolver for testcontainers-based etcd clusters.
 */
public class EtcdClusterEndpointResolver implements EndpointResolver {
    private final AddressResolver resolver;
    private final Address target;

    /**
     * Creates an endpoint resolver for a testcontainers etcd cluster.
     *
     * @param  cluster the etcd cluster
     * @return         an endpoint resolver
     */
    public static EtcdClusterEndpointResolver create(EtcdCluster cluster) {
        List<SocketAddress> addresses = cluster.containers().stream()
            .map(EtcdContainer::getClientAddress)
            .map(addr -> SocketAddress.inetSocketAddress(addr.getPort(), addr.getHostName()))
            .collect(Collectors.toList());

        AddressResolver resolver = AddressResolver.mappingResolver(ignored -> addresses);
        Address target = SocketAddress.inetSocketAddress(2379, "etcd-test-cluster");

        return new EtcdClusterEndpointResolver(resolver, target);
    }

    private EtcdClusterEndpointResolver(AddressResolver resolver, Address target) {
        this.resolver = resolver;
        this.target = target;
    }

    @Override
    public AddressResolver getResolver() {
        return resolver;
    }

    @Override
    public Address getTarget() {
        return target;
    }
}
