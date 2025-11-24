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

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.InternetProtocol;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;

import io.vertx.core.Vertx;
import io.vertx.core.dns.DnsClient;
import io.vertx.core.dns.DnsClientOptions;
import io.vertx.core.dns.SrvRecord;

import io.etcd.jetcd.launcher.EtcdContainer;
import io.etcd.jetcd.resolver.EndpointResolver;
import io.etcd.jetcd.resolver.EndpointResolvers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for DNS SRV resolution using dnsmasq testcontainer.
 * 
 * <p>This test validates that DNS SRV record resolution works correctly
 * by setting up a dnsmasq server and querying it directly with Vert.x DnsClient.
 * This is a focused test that verifies DNS resolution only, without requiring
 * actual etcd connections.
 * 
 * <p>The test uses dnsmasq configured via command-line arguments with proper
 * flags to prevent forwarding issues:
 * <ul>
 *   <li>{@code --no-daemon} - Run in foreground</li>
 *   <li>{@code --no-resolv} - Don't read /etc/resolv.conf</li>
 *   <li>{@code --no-hosts} - Don't read /etc/hosts</li>
 *   <li>{@code --log-queries} - Enable query logging for debugging</li>
 * </ul>
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class DnsSrvIntegrationTest {

    private static GenericContainer<?> dnsmasqContainer;
    private static int dnsPort;
    private static EtcdContainer singleNodeEtcd;

    @BeforeAll
    public static void setupDnsmasq() {
        System.out.println("=== Starting DNS SRV Integration Test Setup ===");

        setupSingleNodeEtcd();
        setupDnsmasqWithEtcdRecords();

        System.out.println("=== Setup Complete ===\n");
    }

    private static void setupSingleNodeEtcd() {
        singleNodeEtcd = new EtcdContainer("quay.io/coreos/etcd:v3.5.15", "etcd0", List.of("etcd0"))
            .withShouldMountDataDirectory(false);

        singleNodeEtcd.start();

        System.out.println("Single-node etcd started on: " +
            singleNodeEtcd.getHost() + ":" + singleNodeEtcd.getMappedPort(2379));
    }

    private static void setupDnsmasqWithEtcdRecords() {
        List<String> dnsmasqArgs = new ArrayList<>();
        dnsmasqArgs.add("--no-daemon");
        dnsmasqArgs.add("--no-resolv");
        dnsmasqArgs.add("--no-hosts");
        dnsmasqArgs.add("--log-queries");

        // Single-node cluster: _etcd._tcp.single.test.local
        int singlePort = singleNodeEtcd.getMappedPort(2379);
        dnsmasqArgs.add("--srv-host=_etcd._tcp.single.test.local,etcd-single.test.local," + singlePort + ",0,0");
        dnsmasqArgs.add("--host-record=etcd-single.test.local,127.0.0.1");

        System.out.println("dnsmasq arguments: " + String.join(" ", dnsmasqArgs));

        dnsPort = findAvailablePort(15353);

        dnsmasqContainer = new GenericContainer<>(DockerImageName.parse("andyshinn/dnsmasq:2.83"))
            .withCommand(dnsmasqArgs.toArray(new String[0]))
            .withCreateContainerCmdModifier(cmd -> {
                cmd.withExposedPorts(
                    new ExposedPort(53, InternetProtocol.UDP)
                );
                cmd.getHostConfig().withPortBindings(
                    new PortBinding(
                        Ports.Binding.bindPort(dnsPort),
                        new ExposedPort(53, InternetProtocol.UDP)
                    )
                );
            });

        dnsmasqContainer.start();
        System.out.println("dnsmasq started on: 127.0.0.1:" + dnsPort);
    }

    @AfterAll
    public static void teardown() {
        System.out.println("=== Tearing Down Test Infrastructure ===");

        if (dnsmasqContainer != null) {
            dnsmasqContainer.stop();
        }

        if (singleNodeEtcd != null) {
            singleNodeEtcd.stop();
        }

        System.out.println("=== Teardown Complete ===");
    }

    private static int findAvailablePort(int startPort) {
        int port = startPort;
        while (!isPortAvailable(port)) {
            port++;
        }
        return port;
    }

    private static boolean isPortAvailable(int port) {
        try (ServerSocket socket = new ServerSocket(port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Test
    public void testDnsSrvResolution() throws Exception {
        Vertx vertx = Vertx.vertx();
        DnsClient dnsClient = vertx.createDnsClient(
            new DnsClientOptions()
                .setHost("127.0.0.1")
                .setPort(dnsPort));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<List<SrvRecord>> result = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();

        dnsClient.resolveSRV("_etcd._tcp.single.test.local")
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    result.set(ar.result());
                } else {
                    error.set(ar.cause());
                }
                latch.countDown();
            });

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        
        if (error.get() != null) {
            System.err.println("DNS query failed: " + error.get().getMessage());
            error.get().printStackTrace();
        }
        
        assertThat(error.get()).isNull();

        List<SrvRecord> srvRecords = result.get();
        assertThat(srvRecords).isNotEmpty();

        SrvRecord record = srvRecords.get(0);
        assertThat(record.target()).isEqualTo("etcd-single.test.local");
        int expectedPort = singleNodeEtcd.getMappedPort(2379);
        assertThat(record.port()).isEqualTo(expectedPort);
        assertThat(record.priority()).isEqualTo(0);
        assertThat(record.weight()).isEqualTo(0);

        System.out.println("DNS SRV resolution test: PASSED");
        System.out.println("  Target: " + record.target());
        System.out.println("  Port: " + record.port());
        System.out.println("  Priority: " + record.priority());
        System.out.println("  Weight: " + record.weight());

        vertx.close();
    }

    @Test
    public void testARecordResolution() throws Exception {
        Vertx vertx = Vertx.vertx();
        DnsClient dnsClient = vertx.createDnsClient(
            new DnsClientOptions()
                .setHost("127.0.0.1")
                .setPort(dnsPort));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<List<String>> result = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();

        dnsClient.resolveA("etcd-single.test.local")
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    result.set(ar.result());
                } else {
                    error.set(ar.cause());
                }
                latch.countDown();
            });

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        
        if (error.get() != null) {
            System.err.println("DNS A query failed: " + error.get().getMessage());
            error.get().printStackTrace();
        }
        
        assertThat(error.get()).isNull();
        assertThat(result.get()).contains("127.0.0.1");

        System.out.println("DNS A record resolution test: PASSED");
        System.out.println("  Resolved: " + result.get());

        vertx.close();
    }

    @Test
    public void testJetcdDnsSrvResolver() throws Exception {
        EndpointResolver resolver = EndpointResolvers.dnsSrv(
            "_etcd._tcp.single.test.local",
            "127.0.0.1",
            dnsPort);

        assertThat(resolver).isNotNull();
        assertThat(resolver.getTarget()).isNotNull();
        assertThat(resolver.getResolver()).isNotNull();

        System.out.println("jetcd DNS SRV resolver created successfully");
    }

}
