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

import java.util.stream.Collectors;

import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.launcher.EtcdContainer;
import io.etcd.jetcd.resolver.AbstractEndpointResolver;
import io.vertx.core.net.Address;
import io.vertx.core.net.AddressResolver;
import io.vertx.core.net.SocketAddress;

/**
 * An endpoint resolver for testcontainers-based etcd clusters.
 */
public class EtcdClusterEndpointResolver extends AbstractEndpointResolver {

    /**
     * Creates an endpoint resolver for a testcontainers etcd cluster.
     * The resolver dynamically queries fresh endpoints on each resolution attempt,
     * ensuring it adapts to cluster restarts and port changes.
     *
     * @param  cluster the etcd cluster
     * @return         an endpoint resolver
     */
    public static EtcdClusterEndpointResolver create(EtcdCluster cluster) {
        // Create resolver that queries fresh endpoints on each resolution
        AddressResolver resolver = AddressResolver.mappingResolver(ignored -> {
            return cluster.containers().stream()
                .map(EtcdContainer::getClientAddress)
                .map(addr -> SocketAddress.inetSocketAddress(addr.getPort(), addr.getHostName()))
                .collect(Collectors.toList());
        });

        Address target = SocketAddress.inetSocketAddress(2379, "etcd-test-cluster");

        return new EtcdClusterEndpointResolver(resolver, target);
    }

    private EtcdClusterEndpointResolver(AddressResolver resolver, Address target) {
        super(resolver, target);
    }
}
