# Load Balancing in jetcd

## Overview

jetcd supports client-side load balancing to distribute requests across multiple etcd cluster nodes. This enhances fault tolerance, optimizes resource utilization, and improves overall application performance.

## Load Balancing Strategies

jetcd leverages Vert.x's native load balancing capabilities, offering four built-in strategies:

| Strategy | Description | Use Case |
|----------|-------------|----------|
| **ROUND_ROBIN** | Distributes requests evenly across all endpoints in a circular fashion | Default strategy, ideal for most use cases with homogeneous server capabilities |
| **LEAST_REQUESTS** | Routes requests to the endpoint with the fewest active requests | Best for scenarios with varying request latencies or heterogeneous server capabilities |
| **RANDOM** | Randomly selects an endpoint for each request | Simple strategy providing good load distribution with minimal overhead |
| **POWER_OF_TWO_CHOICES** | Picks the better of two randomly selected endpoints based on active requests | Balances randomization benefits with load awareness, good middle ground |

## Configuration

### Default Behavior

If no load balancer is explicitly configured, jetcd defaults to **ROUND_ROBIN**:

```java
// Uses ROUND_ROBIN by default
Client client = Client.builder("http://etcd1:2379", "http://etcd2:2379", "http://etcd3:2379")
    .build();
```

### Explicit Configuration

You can explicitly set the load balancing strategy using the `loadBalancer()` method:

```java
import io.etcd.jetcd.Client;
import io.vertx.core.net.endpoint.LoadBalancer;

// Use LEAST_REQUESTS strategy
Client client = Client.builder("http://etcd1:2379", "http://etcd2:2379", "http://etcd3:2379")
    .loadBalancer(LoadBalancer.LEAST_REQUESTS)
    .build();
```

### With DNS SRV Resolution

Load balancing works seamlessly with DNS SRV-based service discovery:

```java
import io.etcd.jetcd.Client;
import io.etcd.jetcd.resolver.EndpointResolvers;
import io.vertx.core.net.endpoint.LoadBalancer;

// Use DNS SRV with POWER_OF_TWO_CHOICES strategy
Client client = Client.builder(EndpointResolvers.dnsSrv("_etcd._tcp.example.com"))
    .loadBalancer(LoadBalancer.POWER_OF_TWO_CHOICES)
    .build();
```

### With Custom Endpoint Resolver

Load balancing integrates with custom endpoint resolvers:

```java
import io.etcd.jetcd.Client;
import io.etcd.jetcd.resolver.EndpointResolver;
import io.vertx.core.net.endpoint.LoadBalancer;

EndpointResolver customResolver = // ... your custom resolver
Client client = Client.builder(customResolver)
    .loadBalancer(LoadBalancer.RANDOM)
    .build();
```

## Strategy Selection Guide

### When to Use ROUND_ROBIN (Default)

**Characteristics:**
- Predictable distribution pattern
- Low computational overhead
- Fair distribution assuming similar request processing times

**Best For:**
- Homogeneous etcd cluster (all nodes have similar capacity)
- Requests with similar execution times
- General-purpose applications
- When you don't have specific load balancing requirements

**Example:**
```java
Client client = Client.builder("http://etcd1:2379", "http://etcd2:2379", "http://etcd3:2379")
    .loadBalancer(LoadBalancer.ROUND_ROBIN)
    .build();
```

### When to Use LEAST_REQUESTS

**Characteristics:**
- Routes to the server with fewest active connections
- Adaptive to real-time server load
- Higher computational overhead than ROUND_ROBIN

**Best For:**
- Heterogeneous cluster (nodes with different capacities)
- Workloads with highly variable request durations
- When some requests are much slower than others (e.g., watch operations vs. simple gets)
- Scenarios where servers may have different loads from other sources

**Example:**
```java
Client client = Client.builder("http://etcd1:2379", "http://etcd2:2379", "http://etcd3:2379")
    .loadBalancer(LoadBalancer.LEAST_REQUESTS)
    .build();
```

### When to Use RANDOM

**Characteristics:**
- Simple and fast
- Good distribution with large request volumes
- No connection state tracking needed

**Best For:**
- High-throughput scenarios with many small requests
- When you want to minimize load balancer overhead
- Testing and development environments
- Stateless operations

**Example:**
```java
Client client = Client.builder("http://etcd1:2379", "http://etcd2:2379", "http://etcd3:2379")
    .loadBalancer(LoadBalancer.RANDOM)
    .build();
```

### When to Use POWER_OF_TWO_CHOICES

**Characteristics:**
- Combines benefits of random selection with load awareness
- Balances overhead and effectiveness
- Better than random, lighter than least requests

**Best For:**
- General-purpose applications needing better distribution than RANDOM
- When LEAST_REQUESTS overhead is a concern but you want load awareness
- Moderate-scale deployments
- Applications with mixed request patterns

**Example:**
```java
Client client = Client.builder("http://etcd1:2379", "http://etcd2:2379", "http://etcd3:2379")
    .loadBalancer(LoadBalancer.POWER_OF_TWO_CHOICES)
    .build();
```

## Performance Considerations

### Overhead Comparison

From lowest to highest computational overhead:

1. **RANDOM** - Minimal overhead, just random number generation
2. **ROUND_ROBIN** - Low overhead, simple counter increment
3. **POWER_OF_TWO_CHOICES** - Moderate overhead, checks two endpoints
4. **LEAST_REQUESTS** - Higher overhead, tracks all endpoint states

### Effectiveness Comparison

From least to most adaptive to server load:

1. **RANDOM** - No load awareness, purely stochastic
2. **ROUND_ROBIN** - No load awareness, deterministic rotation
3. **POWER_OF_TWO_CHOICES** - Moderate load awareness
4. **LEAST_REQUESTS** - Full load awareness, most adaptive

### Memory Usage

- **ROUND_ROBIN**: Minimal (single counter)
- **RANDOM**: Minimal (no state)
- **POWER_OF_TWO_CHOICES**: Moderate (request count per endpoint)
- **LEAST_REQUESTS**: Moderate (request count per endpoint)

## Advanced Topics

### Custom Load Balancers

You can implement custom load balancing logic by implementing the `io.vertx.core.net.endpoint.LoadBalancer` interface:

```java
import io.vertx.core.net.endpoint.LoadBalancer;
import io.vertx.core.net.endpoint.Endpoint;
import java.util.List;

LoadBalancer customBalancer = new LoadBalancer() {
    @Override
    public Endpoint selectEndpoint(List<Endpoint> endpoints) {
        // Your custom selection logic
        return endpoints.get(0); // Example
    }
};

Client client = Client.builder("http://etcd1:2379", "http://etcd2:2379")
    .loadBalancer(customBalancer)
    .build();
```

### Load Balancing with Failover

Load balancing works in conjunction with jetcd's connection retry and failover mechanisms. If a selected endpoint fails:

1. The request is retried according to the configured retry policy
2. The load balancer may select a different endpoint for the retry
3. Failed endpoints are eventually retried after recovery

### Monitoring Load Distribution

To monitor how requests are distributed across your etcd cluster:

1. Enable etcd server metrics
2. Monitor request counts per server
3. Use etcd's built-in metrics endpoint: `http://etcd-server:2379/metrics`
4. Look for `grpc_server_handled_total` metric

## Best Practices

1. **Start with ROUND_ROBIN**: It's the default for good reason and works well for most scenarios
2. **Profile Before Optimizing**: Measure your application's actual behavior before switching strategies
3. **Consider Your Workload**: Match the strategy to your request patterns (uniform vs. varied)
4. **Test in Production-Like Environment**: Load balancer effectiveness can vary with scale
5. **Monitor Server Health**: Combine load balancing with health checks and monitoring
6. **Use LEAST_REQUESTS for Heterogeneous Clusters**: If your etcd nodes have different capacities
7. **Avoid Premature Optimization**: Don't use complex strategies unless you have a specific need

## Troubleshooting

### Uneven Load Distribution

**Symptoms**: Some etcd servers receive significantly more requests than others

**Possible Causes:**
- Using RANDOM with too few requests (statistically not enough samples)
- DNS caching issues with DNS SRV resolution
- Long-lived watch operations skewing LEAST_REQUESTS metrics

**Solutions:**
- Switch to ROUND_ROBIN for predictable distribution
- Ensure DNS TTL is appropriately configured
- Monitor and verify endpoint discovery is working correctly

### High Client CPU Usage

**Symptoms**: jetcd client consuming excessive CPU

**Possible Causes:**
- LEAST_REQUESTS overhead with very high request rates
- Too many endpoints being tracked

**Solutions:**
- Switch to ROUND_ROBIN or RANDOM
- Reduce the number of endpoints if possible
- Profile the application to confirm load balancer is the cause

### Connection Pool Exhaustion

**Symptoms**: "Too many connections" errors or connection timeouts

**Possible Causes:**
- Load balancer not distributing connections evenly
- Some endpoints receiving too many concurrent requests

**Solutions:**
- Verify load balancer configuration
- Check etcd server connection limits
- Consider using LEAST_REQUESTS to better distribute load
- Tune connection pool settings

## Examples

### Basic Web Application

```java
// Web app with balanced reads and writes
Client client = Client.builder(
        "http://etcd1:2379",
        "http://etcd2:2379",
        "http://etcd3:2379")
    .loadBalancer(LoadBalancer.ROUND_ROBIN)
    .build();
```

### High-Throughput Analytics

```java
// Analytics with many small reads
Client client = Client.builder(
        "http://etcd1:2379",
        "http://etcd2:2379",
        "http://etcd3:2379")
    .loadBalancer(LoadBalancer.RANDOM)
    .build();
```

### Mixed Workload

```java
// Application with varying request types
Client client = Client.builder(
        "http://etcd1:2379",
        "http://etcd2:2379",
        "http://etcd3:2379")
    .loadBalancer(LoadBalancer.POWER_OF_TWO_CHOICES)
    .build();
```

### Heterogeneous Cluster

```java
// Different server capacities
Client client = Client.builder(
        "http://powerful-etcd:2379",
        "http://standard-etcd1:2379",
        "http://standard-etcd2:2379")
    .loadBalancer(LoadBalancer.LEAST_REQUESTS)
    .build();
```

## Related Documentation

- [DNS SRV Resolution](DNS_SRV_RESOLUTION.md) - Dynamic service discovery
- [Endpoint Resolvers](migration-to-vertx-grpc-client.md#endpoint-resolver-architecture) - Custom endpoint resolution
- [Vert.x Load Balancing](https://vertx.io/docs/vertx-core/java/#_load_balancing) - Underlying Vert.x implementation

## References

- [The Power of Two Random Choices](https://www.eecs.harvard.edu/~michaelm/postscripts/handbook2001.pdf) - Academic paper on POWER_OF_TWO_CHOICES algorithm
- [Vert.x Documentation](https://vertx.io/docs/) - Official Vert.x documentation
- [etcd Clustering Guide](https://etcd.io/docs/latest/op-guide/clustering/) - etcd cluster configuration

