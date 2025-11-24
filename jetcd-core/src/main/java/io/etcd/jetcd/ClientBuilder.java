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

package io.etcd.jetcd;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import io.etcd.jetcd.common.exception.EtcdException;
import io.etcd.jetcd.common.exception.EtcdExceptionFactory;
import io.etcd.jetcd.impl.ClientImpl;
import io.etcd.jetcd.resolver.EndpointResolver;
import io.etcd.jetcd.support.Preconditions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.net.endpoint.LoadBalancer;

/**
 * ClientBuilder knows how to create a Client instance.
 */
public final class ClientBuilder implements Cloneable {

    private final EndpointResolver endpointResolver;
    private ByteSequence user;
    private ByteSequence password;
    private ExecutorService executorService;
    private LoadBalancer loadBalancer;
    private Map<String, String> headers;
    private HttpClientOptions httpClientOptions;
    private Integer maxInboundMessageSize;
    private ByteSequence namespace = ByteSequence.EMPTY;
    private long retryDelay = 500;
    private long retryMaxDelay = 2500;
    private int retryMaxAttempts = 2;
    private ChronoUnit retryChronoUnit = ChronoUnit.MILLIS;
    private Duration keepaliveTime = Duration.ofSeconds(30L);
    private Duration keepaliveTimeout = Duration.ofSeconds(10L);
    private Boolean keepaliveWithoutCalls = true;
    private Duration retryMaxDuration;
    private Duration connectTimeout;
    private boolean waitForReady = true;
    private Vertx vertx;

    ClientBuilder(EndpointResolver endpointResolver) {
        this.endpointResolver = Preconditions.requireNonNull(endpointResolver, "endpointResolver cannot be null");
    }

    /**
     * Gets the endpoint resolver.
     *
     * @return the endpoint resolver.
     */
    public EndpointResolver endpointResolver() {
        return endpointResolver;
    }

    /**
     * Returns the auth user
     *
     * @return the user.
     */
    public ByteSequence user() {
        return user;
    }

    /**
     * config etcd auth user.
     *
     * @param  user                 etcd auth user
     * @return                      this builder
     * @throws NullPointerException if user is <code>null</code>
     */
    public ClientBuilder user(ByteSequence user) {
        Objects.requireNonNull(user, "user can't be null");
        this.user = user;
        return this;
    }

    /**
     * Returns the auth password
     *
     * @return the password.
     */
    public ByteSequence password() {
        return password;
    }

    /**
     * config etcd auth password.
     *
     * @param  password             etcd auth password
     * @return                      this builder
     * @throws NullPointerException if password is <code>null</code>
     */
    public ClientBuilder password(ByteSequence password) {
        Objects.requireNonNull(password, "password can't be null");
        this.password = password;
        return this;
    }

    /**
     * Returns the namespace of each key used
     *
     * @return the namespace.
     */
    public ByteSequence namespace() {
        return namespace;
    }

    /**
     * config the namespace of keys used in {@code KV}, {@code Txn}, {@code Lock} and {@code Watch}.
     * "/" will be treated as no namespace.
     *
     * @param  namespace            the namespace of each key used
     * @return                      this builder
     * @throws NullPointerException if namespace is <code>null</code>
     */
    public ClientBuilder namespace(ByteSequence namespace) {
        Objects.requireNonNull(namespace, "namespace can't be null");
        this.namespace = namespace;
        return this;
    }

    /**
     * Returns the executor service
     *
     * @return the executor service.
     */
    public ExecutorService executorService() {
        return executorService;
    }

    /**
     * config executor service.
     *
     * @param  executorService      executor service
     * @return                      this builder
     * @throws NullPointerException if executorService is <code>null</code>
     */
    public ClientBuilder executorService(ExecutorService executorService) {
        Objects.requireNonNull(executorService, "executorService can't be null");
        this.executorService = executorService;
        return this;
    }

    /**
     * Returns the HTTP client options used for configuring the gRPC client.
     *
     * @return the HTTP client options, or null if not configured.
     */
    public HttpClientOptions httpClientOptions() {
        return httpClientOptions;
    }

    /**
     * Configure HTTP client options for the gRPC client.
     *
     * <p>
     * This allows full control over the HTTP/2 client configuration, including SSL/TLS settings,
     * connection pooling, timeouts, and other HTTP client behaviors.
     * </p>
     *
     * <p>
     * Example usage for SSL/TLS configuration:
     * </p>
     *
     * <pre>
     * import io.vertx.core.http.HttpClientOptions;
     * import io.vertx.core.net.PemTrustOptions;
     *
     * Client client = Client.builder("https://localhost:2379")
     *     .httpClientOptions(new HttpClientOptions()
     *         .setSsl(true)
     *         .setUseAlpn(true)
     *         .setTrustOptions(new PemTrustOptions().addCertPath("/path/to/ca.pem"))
     *         .setVerifyHost(false))
     *     .build();
     * </pre>
     *
     * @param  httpClientOptions the HTTP client options
     * @return                   this builder
     */
    public ClientBuilder httpClientOptions(HttpClientOptions httpClientOptions) {
        this.httpClientOptions = httpClientOptions;
        return this;
    }

    /**
     * Configure HTTP client options using a fluent consumer pattern.
     *
     * <p>
     * This method provides a convenient way to configure HTTP client options inline.
     * If HTTP client options were already set, this method will modify the existing
     * options rather than replacing them.
     * </p>
     *
     * <p>
     * Example:
     * </p>
     *
     * <pre>
     * Client client = Client.builder("https://localhost:2379")
     *     .httpClientOptions(options -> options
     *         .setSsl(true)
     *         .setUseAlpn(true)
     *         .setTrustAll(true))
     *     .build();
     * </pre>
     *
     * <p>
     * For SSL/TLS configuration, see {@link io.etcd.jetcd.support.SslUtil} for helper methods:
     * </p>
     *
     * <pre>
     * import io.etcd.jetcd.support.SslUtil;
     *
     * Client client = Client.builder("https://localhost:2379")
     *     .httpClientOptions(SslUtil.withTrustManager("/path/to/ca.pem")
     *         .andThen(options -> options.setVerifyHost(false)))
     *     .build();
     * </pre>
     *
     * @param  consumer a consumer that configures the HttpClientOptions
     * @return          this builder
     * @see             io.etcd.jetcd.support.SslUtil
     */
    public ClientBuilder httpClientOptions(Consumer<HttpClientOptions> consumer) {
        // If options already exist, modify them. Otherwise create new ones.
        HttpClientOptions options = this.httpClientOptions != null ? this.httpClientOptions : new HttpClientOptions();
        consumer.accept(options);
        return httpClientOptions(options);
    }

    /**
     * Returns the maximum message size allowed for a single gRPC frame.
     *
     * @return max inbound message size.
     */
    public Integer maxInboundMessageSize() {
        return maxInboundMessageSize;
    }

    /**
     * Sets the maximum message size allowed for a single gRPC frame.
     *
     * @param  maxInboundMessageSize the maximum message size allowed for a single gRPC frame.
     * @return                       this builder
     */
    public ClientBuilder maxInboundMessageSize(Integer maxInboundMessageSize) {
        this.maxInboundMessageSize = maxInboundMessageSize;
        return this;
    }

    /**
     * Sets the load balancer for distributing requests across etcd endpoints.
     *
     * <p>
     * Available strategies:
     * </p>
     * <ul>
     * <li>LoadBalancer.ROUND_ROBIN: Distributes requests evenly (default)</li>
     * <li>LoadBalancer.LEAST_REQUESTS: Routes to endpoint with fewest active requests</li>
     * <li>LoadBalancer.RANDOM: Random endpoint selection</li>
     * <li>LoadBalancer.POWER_OF_TWO_CHOICES: Picks best of two random endpoints</li>
     * </ul>
     *
     * @param  loadBalancer the load balancer instance
     * @return              this builder
     */
    public ClientBuilder loadBalancer(LoadBalancer loadBalancer) {
        this.loadBalancer = loadBalancer;
        return this;
    }

    /**
     * Returns the load balancer.
     *
     * @return the load balancer.
     */
    public LoadBalancer loadBalancer() {
        return loadBalancer;
    }

    /**
     * Returns the custom headers configured for all gRPC requests.
     *
     * @return an unmodifiable map of custom headers
     */
    public Map<String, String> headers() {
        return headers == null ? Collections.emptyMap() : Collections.unmodifiableMap(headers);
    }

    /**
     * Configure custom headers to be added to all gRPC requests.
     *
     * <p>
     * These headers will be added to every request made to etcd, including authentication
     * requests. Useful for adding correlation IDs, tracing headers, or other custom metadata.
     * </p>
     *
     * <p>
     * Example:
     * </p>
     *
     * <pre>
     * Client client = Client.builder("http://localhost:2379")
     *     .header("X-Request-ID", "abc-123")
     *     .header("X-Trace-ID", "trace-456")
     *     .build();
     * </pre>
     *
     * @param  headers custom headers map
     * @return         this builder
     */
    public ClientBuilder headers(Map<String, String> headers) {
        this.headers = new HashMap<>(headers);
        return this;
    }

    /**
     * Add a single custom header to be included in all gRPC requests.
     *
     * @param  key   header name
     * @param  value header value
     * @return       this builder
     */
    public ClientBuilder header(String key, String value) {
        if (this.headers == null) {
            this.headers = new HashMap<>();
        }
        this.headers.put(key, value);
        return this;
    }

    /**
     * Returns The delay between retries.
     *
     * @return the retry delay.
     */
    public long retryDelay() {
        return retryDelay;
    }

    /**
     * The delay between retries.
     *
     * @param  retryDelay The delay between retries.
     * @return            this builder
     */
    public ClientBuilder retryDelay(long retryDelay) {
        this.retryDelay = retryDelay;
        return this;
    }

    /**
     * Returns the max backing off delay between retries
     *
     * @return max retry delay.
     */
    public long retryMaxDelay() {
        return retryMaxDelay;
    }

    /**
     * Set the max backing off delay between retries.
     *
     * @param  retryMaxDelay The max backing off delay between retries.
     * @return               this builder
     */
    public ClientBuilder retryMaxDelay(long retryMaxDelay) {
        this.retryMaxDelay = retryMaxDelay;
        return this;
    }

    /**
     * Returns the max number of retry attempts
     *
     * @return max retry attempts.
     */
    public int retryMaxAttempts() {
        return retryMaxAttempts;
    }

    /**
     * Set the max number of retry attempts
     *
     * @param  retryMaxAttempts The max retry attempts.
     * @return                  this builder
     */
    public ClientBuilder retryMaxAttempts(int retryMaxAttempts) {
        this.retryMaxAttempts = retryMaxAttempts;
        return this;
    }

    /**
     * Returns the keep alive time.
     *
     * @return keep alive time.
     */
    public Duration keepaliveTime() {
        return keepaliveTime;
    }

    /**
     * The interval for gRPC keepalives.
     * The current minimum allowed by gRPC is 10s
     *
     * @param  keepaliveTime time between keepalives
     * @return               this builder
     */
    public ClientBuilder keepaliveTime(Duration keepaliveTime) {
        // gRPC uses a minimum keepalive time of 10s, if smaller values are given.
        // No check here though, as this gRPC value might change
        this.keepaliveTime = keepaliveTime;
        return this;
    }

    /**
     * Returns the keep alive time out.
     *
     * @return keep alive time out.
     */
    public Duration keepaliveTimeout() {
        return keepaliveTimeout;
    }

    /**
     * The timeout for gRPC keepalives
     *
     * @param  keepaliveTimeout the gRPC keep alive timeout.
     * @return                  this builder
     */
    public ClientBuilder keepaliveTimeout(Duration keepaliveTimeout) {
        this.keepaliveTimeout = keepaliveTimeout;
        return this;
    }

    public Boolean keepaliveWithoutCalls() {
        return keepaliveWithoutCalls;
    }

    /**
     * Keepalive option for gRPC
     *
     * @param  keepaliveWithoutCalls the gRPC keep alive without calls.
     * @return                       this builder
     */
    public ClientBuilder keepaliveWithoutCalls(Boolean keepaliveWithoutCalls) {
        this.keepaliveWithoutCalls = keepaliveWithoutCalls;
        return this;
    }

    /**
     * Returns he retries period unit.
     *
     * @return the chrono unit.
     */
    public ChronoUnit retryChronoUnit() {
        return retryChronoUnit;
    }

    /**
     * Sets the retries period unit.
     *
     * @param  retryChronoUnit the retries period unit.
     * @return                 this builder
     */
    public ClientBuilder retryChronoUnit(ChronoUnit retryChronoUnit) {
        this.retryChronoUnit = retryChronoUnit;
        return this;
    }

    /**
     * Returns the retries max duration.
     *
     * @return retry max duration.
     */
    public Duration retryMaxDuration() {
        return retryMaxDuration;
    }

    /**
     * Returns the connect timeout.
     *
     * @return connect timeout.
     */
    public Duration connectTimeout() {
        return connectTimeout;
    }

    /**
     * Set the retries max duration.
     *
     * @param  retryMaxDuration the retries max duration.
     * @return                  this builder
     */
    public ClientBuilder retryMaxDuration(Duration retryMaxDuration) {
        this.retryMaxDuration = retryMaxDuration;
        return this;
    }

    /**
     * Set the connection timeout.
     *
     * @param  connectTimeout Sets the connection timeout.
     *                        Clients connecting to fault tolerant etcd clusters (eg, clusters with more than 2 etcd server
     *                        peers/endpoints)
     *                        should consider a value that will allow switching timely from a crashed/partitioned peer to
     *                        a consensus peer.
     * @return                this builder
     */
    public ClientBuilder connectTimeout(Duration connectTimeout) {
        if (connectTimeout != null) {
            long millis = connectTimeout.toMillis();
            if ((int) millis != millis) {
                throw new IllegalArgumentException("connectTimeout outside of its bounds, max value: " +
                    Integer.MAX_VALUE);
            }
        }
        this.connectTimeout = connectTimeout;
        return this;
    }

    /**
     * Enable gRPC's wait for ready semantics.
     *
     * @return if this client uses gRPC's wait for ready semantics.
     * @see    <a href="https://github.com/grpc/grpc/blob/master/doc/wait-for-ready.md">gRPC Wait for Ready Semantics</a>
     */
    public boolean waitForReady() {
        return waitForReady;
    }

    /**
     * Configure the gRPC's wait for ready semantics.
     *
     * @param  waitForReady if this client should use gRPC's wait for ready semantics. Enabled by default.
     * @return              this builder.
     * @see                 <a href="https://github.com/grpc/grpc/blob/master/doc/wait-for-ready.md">gRPC Wait for Ready
     *                      Semantics</a>
     */
    public ClientBuilder waitForReady(boolean waitForReady) {
        this.waitForReady = waitForReady;
        return this;
    }

    /**
     * Gets the Vertx instance.
     *
     * @return the vertx instance.
     */
    public Vertx vertx() {
        return vertx;
    }

    /**
     * configure Vertx instance.
     *
     * @param  vertx                    Vertx instance to use.
     * @return                          this builder to train
     * @throws IllegalArgumentException if vertx is null
     */
    public ClientBuilder vertx(Vertx vertx) {
        Preconditions.checkArgument(vertx != null, "vertx can't be null");

        this.vertx = vertx;

        return this;
    }

    /**
     * build a new Client.
     *
     * @return               Client instance.
     * @throws EtcdException if client experiences build error.
     */
    public Client build() {
        return new ClientImpl(this);
    }

    /**
     * Returns a copy of this builder
     *
     * @return a copy of the builder.
     */
    public ClientBuilder copy() {
        try {
            ClientBuilder clone = (ClientBuilder) super.clone();
            // Deep copy the headers map to avoid shared mutable state
            if (this.headers != null) {
                clone.headers = new HashMap<>(this.headers);
            }
            return clone;
        } catch (CloneNotSupportedException e) {
            throw EtcdExceptionFactory.toEtcdException(e);
        }
    }
}
