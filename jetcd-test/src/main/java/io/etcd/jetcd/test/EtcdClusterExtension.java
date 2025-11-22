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

package io.etcd.jetcd.test;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;

import io.etcd.jetcd.launcher.Etcd;
import io.etcd.jetcd.launcher.EtcdCluster;

/**
 * JUnit5 Extension to have etcd cluster in tests.
 */
public class EtcdClusterExtension implements BeforeAllCallback, BeforeEachCallback, AfterAllCallback, AfterEachCallback {

    private static final Map<String, EtcdCluster> CLUSTERS = new ConcurrentHashMap<>();

    private final EtcdCluster cluster;
    private final AtomicBoolean beforeAll;

    private EtcdClusterExtension(EtcdCluster cluster) {
        this.cluster = cluster;
        this.beforeAll = new AtomicBoolean();
    }

    /**
     * Returns the underlying etcd cluster for direct access to cluster operations.
     *
     * @return the etcd cluster instance
     */
    public EtcdCluster cluster() {
        return this.cluster;
    }

    /**
     * Restarts all nodes in the cluster with a delay between restarts.
     * Useful for testing resilience and recovery scenarios.
     *
     * @param delay the delay between node restarts
     * @param unit  the time unit of the delay
     */
    public void restart(long delay, TimeUnit unit) {
        try {
            this.cluster.restart(delay, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the cluster name used to identify this cluster instance.
     *
     * @return the cluster name
     */
    public String clusterName() {
        return this.cluster.clusterName();
    }

    /**
     * Returns the client endpoints for connecting to the cluster.
     *
     * @return list of client endpoint URIs
     */
    public List<URI> clientEndpoints() {
        return this.cluster.clientEndpoints();
    }

    /**
     * Returns the peer endpoints used for cluster member communication.
     *
     * @return list of peer endpoint URIs
     */
    public List<URI> peerEndpoints() {
        return this.cluster.peerEndpoints();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        this.beforeAll.set(true);
        before(context);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        before(context);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        this.beforeAll.set(false);
        after(context);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        after(context);
    }

    protected synchronized void before(ExtensionContext context) {
        EtcdCluster oldCluster = CLUSTERS.putIfAbsent(cluster.clusterName(), cluster);
        if (oldCluster == null) {
            cluster.start();
        }
    }

    protected synchronized void after(ExtensionContext context) {
        if (!this.beforeAll.get()) {
            try {
                cluster.close();
            } finally {
                CLUSTERS.remove(cluster.clusterName());
            }
        }
    }

    /**
     * Retrieves a shared cluster by name. This allows multiple test classes
     * to reference the same cluster instance when using @BeforeAll lifecycle.
     *
     * @param  clusterName the name of the cluster to retrieve
     * @return             the cluster instance, or null if not found
     */
    public static EtcdCluster cluster(String clusterName) {
        return CLUSTERS.get(clusterName);
    }

    /**
     * Creates a new builder for configuring an etcd cluster extension.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for configuring and creating an EtcdClusterExtension for JUnit tests.
     * Provides fluent API for setting up testcontainers-based etcd clusters with
     * various configurations including SSL, custom networks, and multiple nodes.
     */
    public static class Builder {
        private final Etcd.Builder builder = new Etcd.Builder();

        /**
         * Sets the cluster name for identification and shared cluster access.
         *
         * @param  clusterName the cluster name
         * @return             this builder
         */
        public Builder withClusterName(String clusterName) {
            builder.withClusterName(clusterName);
            return this;
        }

        /**
         * Sets the container name prefix for etcd nodes.
         *
         * @param  prefix the container name prefix
         * @return        this builder
         */
        public Builder withPrefix(String prefix) {
            builder.withPrefix(prefix);
            return this;
        }

        /**
         * Sets the number of etcd nodes in the cluster.
         * Use multiple nodes to test quorum, leader election, and cluster resilience.
         *
         * @param  nodes the number of nodes (must be positive)
         * @return       this builder
         */
        public Builder withNodes(int nodes) {
            builder.withNodes(nodes);
            return this;
        }

        /**
         * Enables or disables SSL/TLS for secure communication with the cluster.
         * When enabled, the cluster will use auto-generated certificates.
         *
         * @param  ssl true to enable SSL/TLS
         * @return     this builder
         */
        public Builder withSsl(boolean ssl) {
            builder.withSsl(ssl);
            return this;
        }

        /**
         * Enables debug mode with verbose logging.
         *
         * @param  debug true to enable debug mode
         * @return       this builder
         */
        public Builder withDebug(boolean debug) {
            builder.withDebug(debug);
            return this;
        }

        /**
         * Adds additional command-line arguments to pass to etcd.
         *
         * @param  additionalArgs collection of additional arguments
         * @return                this builder
         */
        public Builder withAdditionalArgs(Collection<String> additionalArgs) {
            builder.withAdditionalArgs(additionalArgs);
            return this;
        }

        /**
         * Sets the etcd container image to use.
         *
         * @param  image the Docker image name (e.g., "quay.io/coreos/etcd:v3.5.0")
         * @return       this builder
         */
        public Builder withImage(String image) {
            builder.withImage(image);
            return this;
        }

        /**
         * Sets a custom testcontainers network for the cluster.
         * Useful when the etcd cluster needs to communicate with other containers
         * in the same test scenario.
         *
         * @param  network the testcontainers network
         * @return         this builder
         */
        public Builder withNetwork(Network network) {
            builder.withNetwork(network);
            return this;
        }

        /**
         * Enables mounting of the etcd data directory to the host filesystem.
         * This allows data to persist between container restarts, useful for testing
         * persistence and recovery scenarios.
         *
         * @param  mountDirectory true to mount the data directory
         * @return                this builder
         */
        public Builder withMountDirectory(boolean mountDirectory) {
            builder.withMountedDataDirectory(mountDirectory);
            return this;
        }

        /**
         * Sets the user for etcd authentication testing.
         *
         * @param  user the username
         * @return      this builder
         */
        public Builder withUser(String user) {
            builder.withUser(user);
            return this;
        }

        /**
         * Builds the EtcdClusterExtension with the configured options.
         *
         * @return the configured extension instance
         */
        public EtcdClusterExtension build() {
            return new EtcdClusterExtension(builder.build());
        }
    }
}
