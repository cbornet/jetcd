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

import io.etcd.jetcd.Auth;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.Cluster;
import io.etcd.jetcd.Election;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.Maintenance;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.common.suppliers.CloseableSupplier;
import io.etcd.jetcd.common.suppliers.Suppliers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Etcd Client implementation.
 */
public final class EtcdClient implements Client {
    private static final Logger LOG = LoggerFactory.getLogger(EtcdClient.class);

    private final GrpcService grpcService;
    private final CloseableSupplier<KV> kvClient;
    private final CloseableSupplier<Auth> authClient;
    private final CloseableSupplier<Maintenance> maintenanceClient;
    private final CloseableSupplier<Cluster> clusterClient;
    private final CloseableSupplier<Lease> leaseClient;
    private final CloseableSupplier<Watch> watchClient;
    private final CloseableSupplier<Lock> lockClient;
    private final CloseableSupplier<Election> electionClient;

    public EtcdClient(ClientBuilder clientBuilder) {
        this.grpcService = new GrpcService(clientBuilder.copy());
        this.kvClient = Suppliers.memoizingCloseable(() -> new KVImpl(this.grpcService));
        this.authClient = Suppliers.memoizingCloseable(() -> new AuthService(this.grpcService));
        this.maintenanceClient = Suppliers.memoizingCloseable(() -> new MaintenanceImpl(this.grpcService));
        this.clusterClient = Suppliers.memoizingCloseable(() -> new ClusterService(this.grpcService));
        this.leaseClient = Suppliers.memoizingCloseable(() -> new LeaseImpl(this.grpcService));
        this.watchClient = Suppliers.memoizingCloseable(() -> new WatchImpl(this.grpcService));
        this.lockClient = Suppliers.memoizingCloseable(() -> new LockImpl(this.grpcService));
        this.electionClient = Suppliers.memoizingCloseable(() -> new ElectionImpl(this.grpcService));
    }

    @Override
    public Auth getAuthClient() {
        return authClient.get();
    }

    @Override
    public KV getKVClient() {
        return kvClient.get();
    }

    @Override
    public Cluster getClusterClient() {
        return clusterClient.get();
    }

    @Override
    public Maintenance getMaintenanceClient() {
        return maintenanceClient.get();
    }

    @Override
    public Lease getLeaseClient() {
        return leaseClient.get();
    }

    @Override
    public Watch getWatchClient() {
        return watchClient.get();
    }

    @Override
    public Lock getLockClient() {
        return lockClient.get();
    }

    @Override
    public Election getElectionClient() {
        return electionClient.get();
    }

    @Override
    public synchronized void close() {
        try {
            closeAsync().get(15, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            LOG.warn("Timeout waiting for Client to close, forcing shutdown");
        } catch (Exception e) {
            LOG.error("Error closing Client", e);
        }

        try {
            grpcService.close().get(15, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            LOG.warn("Timeout waiting for GrpcService to close, forcing shutdown");
        } catch (Exception e) {
            LOG.error("Error closing Client", e);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> closeAsync() {
        List<CompletableFuture<Void>> closeFutures = new ArrayList<>();

        closeFutures.add(closeClientSupplier(authClient, "authClient"));
        closeFutures.add(closeClientSupplier(kvClient, "kvClient"));
        closeFutures.add(closeClientSupplier(clusterClient, "clusterClient"));
        closeFutures.add(closeClientSupplier(maintenanceClient, "maintenanceClient"));
        closeFutures.add(closeClientSupplier(leaseClient, "leaseClient"));
        closeFutures.add(closeClientSupplier(watchClient, "watchClient"));
        closeFutures.add(closeClientSupplier(lockClient, "lockClient"));
        closeFutures.add(closeClientSupplier(electionClient, "electionClient"));

        return CompletableFuture.allOf(closeFutures.toArray(new CompletableFuture[0]))
            .whenComplete((v, error) -> {
                try {
                    grpcService.close().get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    LOG.error("Error closing grpcService", e);
                }
            });
    }

    private CompletableFuture<Void> closeClientSupplier(CloseableSupplier<?> supplier, String name) {
        return CompletableFuture.runAsync(() -> {
            try {
                supplier.close();
            } catch (Exception e) {
                LOG.warn("Error closing {}", name, e);
            }
        });
    }
}

