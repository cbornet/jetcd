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

package io.etcd.jetcd.resolver.dnssrv;

import io.netty.handler.logging.ByteBufFormat;
import io.vertx.core.dns.DnsClientOptions;

/**
 * DNS client options specific to DNS SRV resolution.
 * Wraps Vert.x DnsClientOptions with DNS SRV-specific configuration.
 */
public final class DnsSrvClientOptions {
    private final DnsClientOptions dnsOptions;
    private String serviceName;
    private int minTTL;

    public DnsSrvClientOptions(String serviceName) {
        this(new DnsClientOptions(), serviceName);
    }

    public DnsSrvClientOptions(DnsClientOptions base, String serviceName) {
        this.serviceName = serviceName;
        this.minTTL = 30;
        this.dnsOptions = base != null ? copyDnsClientOptions(base) : new DnsClientOptions();
    }

    public String getServiceName() {
        return serviceName;
    }

    public DnsSrvClientOptions setServiceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    public int getMinTTL() {
        return minTTL;
    }

    /**
     * Sets the minimum TTL (Time To Live) in seconds for DNS SRV record refresh.
     * This value is used as a lower bound when DNS servers return low or zero TTL values.
     *
     * <p>Default: 30 seconds
     *
     * <p>Use cases:
     * <ul>
     *   <li>DNS servers like dnsmasq often return TTL=0 to indicate "don't cache".
     *       Without a minimum TTL, endpoints would never be refreshed.</li>
     *   <li>Set to 0 to disable the minimum and use DNS server's TTL exactly as provided.</li>
     *   <li>Increase (e.g., to 60) for less frequent DNS queries in stable environments.</li>
     * </ul>
     *
     * @param minTTL minimum TTL in seconds (0 or positive value)
     * @return this instance for fluent API
     */
    public DnsSrvClientOptions setMinTTL(int minTTL) {
        this.minTTL = minTTL;
        return this;
    }

    public String getHost() {
        return dnsOptions.getHost();
    }

    public DnsSrvClientOptions setHost(String host) {
        dnsOptions.setHost(host);
        return this;
    }

    public int getPort() {
        return dnsOptions.getPort();
    }

    public DnsSrvClientOptions setPort(int port) {
        dnsOptions.setPort(port);
        return this;
    }

    public long getQueryTimeout() {
        return dnsOptions.getQueryTimeout();
    }

    public DnsSrvClientOptions setQueryTimeout(long queryTimeout) {
        dnsOptions.setQueryTimeout(queryTimeout);
        return this;
    }

    public boolean getLogActivity() {
        return dnsOptions.getLogActivity();
    }

    public DnsSrvClientOptions setLogActivity(boolean logActivity) {
        dnsOptions.setLogActivity(logActivity);
        return this;
    }

    public ByteBufFormat getActivityLogFormat() {
        return dnsOptions.getActivityLogFormat();
    }

    public DnsSrvClientOptions setActivityLogFormat(ByteBufFormat activityLogFormat) {
        dnsOptions.setActivityLogFormat(activityLogFormat);
        return this;
    }

    public boolean isRecursionDesired() {
        return dnsOptions.isRecursionDesired();
    }

    public DnsSrvClientOptions setRecursionDesired(boolean recursionDesired) {
        dnsOptions.setRecursionDesired(recursionDesired);
        return this;
    }

    public DnsClientOptions getDnsOptions() {
        return dnsOptions;
    }

    private static DnsClientOptions copyDnsClientOptions(DnsClientOptions source) {
        DnsClientOptions copy = new DnsClientOptions();
        copy.setHost(source.getHost());
        copy.setPort(source.getPort());
        copy.setQueryTimeout(source.getQueryTimeout());
        copy.setLogActivity(source.getLogActivity());
        copy.setActivityLogFormat(source.getActivityLogFormat());
        copy.setRecursionDesired(source.isRecursionDesired());

        return copy;
    }
}
