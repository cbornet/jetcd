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
import io.etcd.jetcd.common.suppliers.Suppliers;
import io.etcd.jetcd.common.suppliers.CloseableSupplier;

/**
 * Etcd Client.
 */
public final class ClientImpl implements Client {

    private final ClientConnectionManager connectionManager;
    private final CloseableSupplier<KV> kvClient;
    private final CloseableSupplier<Auth> authClient;
    private final CloseableSupplier<Maintenance> maintenanceClient;
    private final CloseableSupplier<Cluster> clusterClient;
    private final CloseableSupplier<Lease> leaseClient;
    private final CloseableSupplier<Watch> watchClient;
    private final CloseableSupplier<Lock> lockClient;
    private final CloseableSupplier<Election> electionClient;

    public ClientImpl(ClientBuilder clientBuilder) {
        this.connectionManager = new ClientConnectionManager(clientBuilder.copy());
        this.kvClient = Suppliers.memoizingCloseable(() -> new KVImpl(this.connectionManager));
        this.authClient = Suppliers.memoizingCloseable(() -> new AuthImpl(this.connectionManager));
        this.maintenanceClient = Suppliers.memoizingCloseable(() -> new MaintenanceImpl(this.connectionManager));
        this.clusterClient = Suppliers.memoizingCloseable(() -> new ClusterImpl(this.connectionManager));
        this.leaseClient = Suppliers.memoizingCloseable(() -> new LeaseImpl(this.connectionManager));
        this.watchClient = Suppliers.memoizingCloseable(() -> new WatchImpl(this.connectionManager));
        this.lockClient = Suppliers.memoizingCloseable(() -> new LockImpl(this.connectionManager));
        this.electionClient = Suppliers.memoizingCloseable(() -> new ElectionImpl(this.connectionManager));
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
            authClient.close();
        } catch (Exception e) {
            // Ignore
        }
        try {
            kvClient.close();
        } catch (Exception e) {
            // Ignore
        }
        try {
            clusterClient.close();
        } catch (Exception e) {
            // Ignore
        }
        try {
            maintenanceClient.close();
        } catch (Exception e) {
            // Ignore
        }
        try {
            leaseClient.close();
        } catch (Exception e) {
            // Ignore
        }
        try {
            watchClient.close();
        } catch (Exception e) {
            // Ignore
        }
        try {
            lockClient.close();
        } catch (Exception e) {
            // Ignore
        }
        try {
            electionClient.close();
        } catch (Exception e) {
            // Ignore
        }

        connectionManager.close();
    }
}
