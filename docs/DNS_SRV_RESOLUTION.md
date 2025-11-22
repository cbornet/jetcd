# DNS SRV Resolution in jetcd

## Overview

jetcd supports DNS SRV (Service) records for service discovery, allowing you to dynamically discover etcd endpoints through DNS queries instead of hardcoding server addresses. This is particularly useful in dynamic cloud environments where server addresses may change.

## What is DNS SRV?

DNS SRV records provide service discovery through DNS. They allow you to query for a service name (e.g., `_etcd._tcp.example.com`) and receive a list of server addresses with priority and weight information for load balancing.

## Using DNS SRV Resolution

### Basic Usage with Default DNS Server

The simplest way to use DNS SRV resolution is with the `EndpointResolvers` helper class:

```java
import io.etcd.jetcd.Client;
import io.etcd.jetcd.resolver.EndpointResolvers;

// Create a client using DNS SRV resolution with default DNS server
Client client = Client.builder(EndpointResolvers.dnsSrv("_etcd._tcp.example.com"))
    .build();
```

### Using a Custom DNS Server

If you need to use a specific DNS server (e.g., in a private network):

```java
import io.etcd.jetcd.Client;
import io.etcd.jetcd.resolver.EndpointResolvers;

// Create a client using DNS SRV resolution with custom DNS server
Client client = Client.builder(EndpointResolvers.dnsSrv(
        "_etcd._tcp.example.com",
        "dns.internal.example.com",
        53))
    .build();
```

### Advanced: Using the Resolver Directly

For more control, you can create the resolver directly and configure it further:

```java
import io.etcd.jetcd.Client;
import io.etcd.jetcd.resolver.EndpointResolver;
import io.etcd.jetcd.resolver.EndpointResolvers;

// Create resolver
EndpointResolver resolver = EndpointResolvers.dnsSrv("_etcd._tcp.example.com");

// Create client with the resolver
Client client = Client.builder(resolver)
    .build();
```

## DNS SRV Record Setup

To use DNS SRV resolution, you need to configure DNS SRV records for your etcd cluster.

### Example DNS Zone Configuration

```dns
; SRV record format: service.proto.name TTL class SRV priority weight port target
_etcd._tcp.example.com. 300 IN SRV 0 0 2379 etcd-1.example.com.
_etcd._tcp.example.com. 300 IN SRV 0 0 2379 etcd-2.example.com.
_etcd._tcp.example.com. 300 IN SRV 0 0 2379 etcd-3.example.com.

; A records for the targets
etcd-1.example.com. 300 IN A 10.0.1.10
etcd-2.example.com. 300 IN A 10.0.1.11
etcd-3.example.com. 300 IN A 10.0.1.12
```

### Testing DNS SRV Records

You can verify your DNS SRV configuration using `dig` or `nslookup`:

```bash
# Using dig
dig SRV _etcd._tcp.example.com

# Using nslookup
nslookup -type=SRV _etcd._tcp.example.com
```

## Kubernetes Example

In Kubernetes, you can use a headless service to automatically create DNS SRV records:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: etcd
  namespace: default
spec:
  clusterIP: None  # Headless service
  ports:
  - name: etcd-client
    port: 2379
    protocol: TCP
  selector:
    app: etcd
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: etcd
  namespace: default
spec:
  serviceName: etcd
  replicas: 3
  selector:
    matchLabels:
      app: etcd
  template:
    metadata:
      labels:
        app: etcd
    spec:
      containers:
      - name: etcd
        image: quay.io/coreos/etcd:latest
        ports:
        - containerPort: 2379
          name: client
```

Then use DNS SRV resolution in your application:

```java
import io.etcd.jetcd.Client;
import io.etcd.jetcd.resolver.EndpointResolvers;

Client client = Client.builder(
        EndpointResolvers.dnsSrv("_etcd-client._tcp.etcd.default.svc.cluster.local"))
    .build();
```

## Comparison with Static Endpoints

### Static Endpoints

```java
import io.etcd.jetcd.resolver.EndpointResolvers;

// Hardcoded addresses (convenience method)
Client client = Client.builder("http://etcd-1:2379", "http://etcd-2:2379", "http://etcd-3:2379")
    .build();

// Or explicitly using EndpointResolvers
Client client = Client.builder(EndpointResolvers.endpoints(
        "http://etcd-1:2379", "http://etcd-2:2379", "http://etcd-3:2379"))
    .build();
```

**Pros:**
- Simple and straightforward
- No DNS dependency
- Predictable behavior

**Cons:**
- Requires code changes when servers change
- No dynamic discovery
- Manual load balancing configuration

### DNS SRV Resolution

```java
import io.etcd.jetcd.resolver.EndpointResolvers;

// Dynamic discovery
Client client = Client.builder(EndpointResolvers.dnsSrv("_etcd._tcp.example.com"))
    .build();
```

**Pros:**
- Dynamic endpoint discovery
- No code changes when servers change
- Supports priority and weight-based selection
- Works well with orchestration platforms

**Cons:**
- Requires DNS infrastructure
- Additional network dependency
- Slightly higher initial connection time

## Troubleshooting

### "Failed to resolve DNS SRV"

**Cause**: DNS server cannot find the SRV record.

**Solutions**:
1. Verify SRV records exist: `dig SRV _etcd._tcp.example.com`
2. Check DNS server configuration
3. Ensure the service name format is correct: `_service._proto.domain`
4. Try using a custom DNS server if the default is not accessible

### "Connection refused" after DNS resolution

**Cause**: DNS resolution succeeded, but etcd servers are not reachable.

**Solutions**:
1. Verify etcd servers are running
2. Check firewall rules allow access to port 2379
3. Verify the target addresses in SRV records are correct
4. Test connectivity: `telnet etcd-1.example.com 2379`

### Slow connection establishment

**Cause**: DNS query timeout or slow response.

**Solutions**:
1. Use a closer DNS server
2. Increase DNS cache TTL
3. Consider using static endpoints for latency-sensitive applications

## Implementation Details

jetcd uses Vert.x's `SrvResolver` from the `vertx-service-resolver` library for DNS SRV resolution. The resolver:

- Performs asynchronous DNS queries
- Caches resolved addresses according to DNS TTL
- Automatically handles priority and weight from SRV records
- Supports custom DNS server configuration
- Works with Vert.x's load balancing infrastructure

## Related Documentation

- [Vert.x Service Resolver](https://vertx.io/docs/vertx-service-resolver/java/)
- [etcd Service Discovery](https://etcd.io/docs/latest/op-guide/clustering/)
- [RFC 2762 - DNS SRV Records](https://datatracker.ietf.org/doc/html/rfc2782)

