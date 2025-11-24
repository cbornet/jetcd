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

package io.etcd.jetcd.launcher;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import static java.util.stream.Collectors.toList;

public class EtcdClusterImpl implements EtcdCluster {
    private static final Logger LOG = LoggerFactory.getLogger(EtcdClusterImpl.class);
    
    private final List<EtcdContainer> containers;
    private final String clusterName;
    private final List<String> endpoints;
    private final Network network;
    private final long startupTimeout;
    private final TimeUnit startupTimeoutUnit;

    public EtcdClusterImpl(
        String image,
        String clusterName,
        String prefix,
        int nodes,
        boolean ssl,
        boolean debug,
        Collection<String> additionalArgs,
        Network network,
        boolean shouldMountDataDirectory,
        String user,
        long startupTimeout,
        TimeUnit startupTimeoutUnit) {

        this.clusterName = clusterName;
        this.startupTimeout = startupTimeout;
        this.startupTimeoutUnit = startupTimeoutUnit;
        this.endpoints = IntStream.range(0, nodes)
            .mapToObj(i -> (prefix == null ? "etcd" : prefix + "etcd") + i)
            .collect(toList());

        // Store network reference for cleanup. If null, use Network.SHARED as default
        this.network = network != null ? network : Network.SHARED;

        this.containers = endpoints.stream()
            .map(e -> new EtcdContainer(image, e, endpoints)
                .withClusterToken(clusterName)
                .withSsl(ssl)
                .withDebug(debug)
                .withAdditionalArgs(additionalArgs)
                .withNetwork(this.network)
                .withShouldMountDataDirectory(shouldMountDataDirectory)
                .withUser(user))
            .collect(toList());
    }

    @Override
    public void start() {
        ExecutorService executor = Executors.newFixedThreadPool(containers.size());

        try {
            List<CompletableFuture<Void>> futures = containers.stream()
                .map(container -> CompletableFuture.runAsync(container::start, executor))
                .collect(toList());

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .orTimeout(startupTimeout, startupTimeoutUnit)
                .join();

        } catch (CompletionException e) {
            try {
                stop();
            } catch (Exception stopEx) {
                LOG.warn("Failed to cleanup containers after startup failure", stopEx);
            }
            Throwable cause = e.getCause();
            if (cause instanceof TimeoutException) {
                throw new EtcdClusterTimeoutException(
                    "Cluster startup timed out after " + startupTimeout + " " + startupTimeoutUnit, cause);
            }
            throw new EtcdClusterStartException("Cluster failed to start", cause);
        } catch (CancellationException e) {
            Thread.currentThread().interrupt();
            try {
                stop();
            } catch (Exception stopEx) {
                LOG.warn("Failed to cleanup containers after interruption", stopEx);
            }
            throw new EtcdClusterStartException("Interrupted while starting cluster", e);
        } finally {
            executor.shutdownNow();
        }
    }

    @Override
    public void stop() {
        List<Exception> failures = new ArrayList<>();
        for (EtcdContainer container : containers) {
            try {
                container.stop();
            } catch (Exception e) {
                LOG.warn("Failed to stop container {}", container.node(), e);
                failures.add(e);
            }
        }
        if (!failures.isEmpty()) {
            LOG.error("Failed to stop {} container(s)", failures.size());
        }
    }

    @Override
    public void close() {
        // Close containers first
        for (EtcdContainer container : containers) {
            container.close();
        }

        // Try to clean up network if it's not the shared one
        // Network.SHARED is managed by Testcontainers and should never be closed
        if (network != null && network != Network.SHARED) {
            try {
                network.close();
                LOG.debug("Successfully closed network for cluster: {}", clusterName);
            } catch (Exception e) {
                // Log but don't fail - cleanup is best-effort
                // This is a fallback for when Ryuk service is not active
                LOG.warn("Failed to cleanup network for cluster {}: {}", clusterName, e.getMessage());
            }
        }
    }

    @Override
    public String clusterName() {
        return clusterName;
    }

    @Override
    public List<URI> clientEndpoints() {
        return containers.stream().map(EtcdContainer::clientEndpoint).collect(toList());
    }

    @Override
    public List<URI> peerEndpoints() {
        return containers.stream().map(EtcdContainer::peerEndpoint).collect(toList());
    }

    @Override
    public List<EtcdContainer> containers() {
        return Collections.unmodifiableList(containers);
    }
}
