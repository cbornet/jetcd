# DNS SRV Resolution Implementation - Status Document

**Last Updated**: November 24, 2025  
**Branch**: `vertx-client`

## Project Context

### Background
The jetcd project is undergoing a migration from `vertx-grpc` (which wraps `grpc-java`) to native `vertx-grpc-client` to resolve Netty dependency conflicts. This migration requires reimplementing service discovery features, including DNS SRV resolution for dynamic etcd endpoint discovery.

### Goals
- Provide DNS SRV-based service discovery for etcd clusters
- Maintain compatibility with existing static endpoint configuration
- Eliminate dependency on external `vertx-service-resolver` library
- Implement custom DNS resolution that works seamlessly with Vert.x gRPC client

## Completed Work

### 1. Custom DNS SRV Resolver Implementation

**Key File**: `jetcd-core/src/main/java/io/etcd/jetcd/resolver/DnsSrvAddressResolver.java`

Implemented a custom `AddressResolver<SocketAddress>` that:
- Performs DNS SRV lookups using Vert.x `DnsClient`
- Returns `SocketAddress` instances (required by gRPC stubs)
- Caches `EndpointResolver` instances per `Vertx` instance using `ConcurrentHashMap`
- Uses blocking DNS resolution (`.get()` on Future) within `AddressResolver.mappingResolver`

```java
public final class DnsSrvAddressResolver implements AddressResolver<SocketAddress> {
    private final String serviceName;
    private final DnsClientOptions dnsOptions;
    private final ConcurrentMap<Vertx, EndpointResolver<SocketAddress, ?, ?, ?>> resolverCache;

    @Override
    public EndpointResolver<SocketAddress, ?, ?, ?> endpointResolver(Vertx vertx) {
        return resolverCache.computeIfAbsent(vertx, this::createEndpointResolver);
    }
    
    private EndpointResolver<SocketAddress, ?, ?, ?> createEndpointResolver(Vertx vertx) {
        final DnsClient dnsClient = vertx.createDnsClient(dnsOptions);
        AddressResolver<SocketAddress> resolver = AddressResolver.mappingResolver(sockAddr -> {
            // Blocking DNS SRV query
            List<SrvRecord> srvRecords = dnsClient.resolveSRV(serviceName)
                .toCompletionStage().toCompletableFuture().get();
            // Convert to SocketAddress list
            return srvRecords.stream()
                .map(srv -> SocketAddress.inetSocketAddress(srv.port(), srv.target()))
                .collect(Collectors.toList());
        });
        return resolver.endpointResolver(vertx);
    }
}
```

### 2. EndpointResolver Interface Update

**File**: `jetcd-core/src/main/java/io/etcd/jetcd/resolver/EndpointResolver.java`

Changed `getTarget()` return type from `SocketAddress` to `Address` (superinterface) to accommodate both:
- `SocketAddress` for static endpoints
- Potential `ServiceAddress` for DNS-based resolution

**Impact**: Required explicit casts to `SocketAddress` in client implementation classes (KVImpl, AuthImpl, etc.)

### 3. Factory Methods

**File**: `jetcd-core/src/main/java/io/etcd/jetcd/resolver/EndpointResolvers.java`

Added `DnsSrv` nested class with factory methods:
```java
public static DnsSrv dnsSrv(String serviceName)
public static DnsSrv dnsSrv(String serviceName, String dnsServer, int dnsPort)
```

### 4. Integration Tests

**File**: `jetcd-core/src/test/java/io/etcd/jetcd/impl/DnsSrvIntegrationTest.java`

Created comprehensive test suite using:
- **dnsmasq** Docker container (andyshinn/dnsmasq:2.83) for DNS server
- **Testcontainers** for infrastructure setup
- **Single-node etcd cluster** for KV operations testing
- **Vert.x DnsClient** for direct DNS queries

### 5. Dependency Cleanup

**Removed**: `io.vertx:vertx-service-resolver` from:
- `jetcd-core/build.gradle`
- `gradle/libs.versions.toml`

**Rationale**: Custom implementation provides better control and eliminates the `ServiceAddress`/`SocketAddress` casting issue.

## Architecture Decisions

### 1. Address Type Strategy
**Decision**: Use `Address` in interface, cast to `SocketAddress` in implementations  
**Reason**: gRPC client stubs require `SocketAddress`, but DNS resolver ecosystem uses `Address`  
**Trade-off**: Requires explicit casts but maintains type safety where needed

### 2. Caching Strategy
**Decision**: Per-Vertx instance caching using `ConcurrentHashMap.computeIfAbsent`  
**Reason**: Each `Vertx` instance may have different configuration; ensure thread-safe lazy initialization  
**Implementation**: `Map<Vertx, EndpointResolver<SocketAddress, ?, ?, ?>>`

### 3. DNS Resolution Approach
**Decision**: Blocking resolution using `.toCompletionStage().toCompletableFuture().get()`  
**Reason**: `AddressResolver.mappingResolver` expects synchronous function  
**Trade-off**: Blocks the calling thread; may impact performance in high-throughput scenarios

## Test Status

### Passing Tests (3/4) ✅

1. **`testDnsSrvResolution()`**
   - Direct DNS SRV query using Vert.x DnsClient
   - Verifies dnsmasq correctly serves SRV records
   - Validates SRV record parsing (target, port, priority, weight)

2. **`testARecordResolution()`**
   - DNS A record resolution for hostnames from SRV targets
   - Confirms A record → IP address mapping

3. **`testJetcdDnsSrvResolver()`**
   - API verification test
   - Ensures `EndpointResolvers.dnsSrv()` creates valid resolver instances
   - Confirms resolver has non-null target and resolver components

### Failing Test (1/4) ❌

**`testJetcdClientWithDnsSrvSingleNode()`** - **TIMEOUT (30 seconds)**

```java
@Test
public void testJetcdClientWithDnsSrvSingleNode() throws Exception {
    EndpointResolver resolver = EndpointResolvers.dnsSrv(
        "_etcd._tcp.single.test.local",
        "127.0.0.1",
        dnsPort);

    try (Client client = Client.builder(resolver).build()) {
        KV kv = client.getKVClient();
        
        ByteSequence key = bytesOf("dns_srv_single_test");
        ByteSequence value = bytesOf("test_value");
        
        kv.put(key, value).get(10, TimeUnit.SECONDS); // TIMES OUT HERE
        // ...
    }
}
```

**Symptoms**:
- Test hangs on first KV operation (`put`)
- No exceptions thrown
- Timeout occurs after 30 seconds (test-level timeout)

## Outstanding Issues

### CRITICAL: KV Test Timeout

**Problem**: jetcd client with DNS SRV resolver times out on KV operations

**Potential Root Causes**:

1. **Blocking DNS Resolution in Event Loop**
   - The `.get()` call in `DnsSrvAddressResolver` blocks the thread
   - If called from Vert.x event loop, could cause deadlock
   - Vert.x event loop might be waiting for DNS resolution that can't complete

2. **Address Resolution Timing**
   - DNS resolution may be triggered at wrong point in connection lifecycle
   - gRPC client might not be invoking the resolver correctly

3. **Load Balancer Integration Issue**
   - Custom resolver may not integrate properly with `LoadBalancer.ROUND_ROBIN`
   - Address list from resolver might not be propagated to load balancer

4. **Network Configuration**
   - Testcontainers networking: etcd on dynamic port, DNS on fixed port
   - SRV record points to `etcd-single.test.local` which resolves to `127.0.0.1`
   - Possible hostname resolution issue in container context

5. **gRPC Client Stub Address Type**
   - Stubs created with `(SocketAddress) endpointResolver.getTarget()`
   - Cast may be losing context needed for dynamic resolution

### Debugging Steps Needed

1. **Add Logging**
   ```java
   // In DnsSrvAddressResolver.createEndpointResolver
   System.out.println("DNS SRV query for: " + serviceName);
   System.out.println("Resolved records: " + srvRecords);
   ```

2. **Test DNS Resolution Separately**
   - Verify DNS queries succeed outside of gRPC context
   - Check timing: how long does DNS query take?

3. **Simplify Test Case**
   - Try connecting without load balancer
   - Try with static SocketAddress directly (bypass resolver)
   - Compare working static endpoint test vs DNS SRV test

4. **Thread Analysis**
   - Check which thread calls `endpointResolver()`
   - Verify if blocking `.get()` is safe on that thread
   - Consider using `vertx.executeBlocking()` for DNS query

5. **Alternative Approaches**
   - Implement fully async resolution (harder, requires API changes)
   - Pre-resolve DNS on client builder (simpler, no dynamic updates)
   - Use separate thread pool for DNS resolution

## Test Infrastructure

### dnsmasq Configuration

```bash
--no-daemon          # Run in foreground
--no-resolv          # Don't read /etc/resolv.conf
--no-hosts           # Don't read /etc/hosts
--log-queries        # Enable query logging
--srv-host=_etcd._tcp.single.test.local,etcd-single.test.local,<PORT>,0,0
--host-record=etcd-single.test.local,127.0.0.1
```

**Port Binding**: UDP port 53 mapped to fixed host port (15353+) to avoid conflicts

### etcd Setup

- **Image**: quay.io/coreos/etcd:v3.5.15
- **Mode**: Single-node cluster
- **Ports**: 2379 (client), 2380 (peer) - dynamically mapped by Testcontainers
- **Configuration**: Data directory not mounted (in-memory)

## API Usage Examples

### Basic DNS SRV Usage

```java
// Using default DNS server (from /etc/resolv.conf)
Client client = Client.builder(
    EndpointResolvers.dnsSrv("_etcd._tcp.example.com")
).build();
```

### Custom DNS Server

```java
// Specify custom DNS server
Client client = Client.builder(
    EndpointResolvers.dnsSrv(
        "_etcd._tcp.example.com",
        "8.8.8.8",  // DNS server
        53          // DNS port
    )
).build();
```

### With Additional Configuration

```java
Client client = Client.builder(EndpointResolvers.dnsSrv("_etcd._tcp.example.com"))
    .loadBalancer(LoadBalancer.ROUND_ROBIN)
    .httpClientOptions(options -> options
        .setSsl(true)
        .setUseAlpn(true))
    .build();
```

## Next Steps (Priority Order)

### Immediate (Today/Tomorrow)

1. **Debug the Timeout Issue**
   - Add comprehensive logging to `DnsSrvAddressResolver`
   - Verify DNS resolution succeeds before KV operation
   - Check thread context and identify deadlock potential

2. **Thread Safety Analysis**
   - Determine which thread invokes `endpointResolver(Vertx)`
   - Verify blocking `.get()` is safe on that thread
   - Consider if event loop thread is being blocked

3. **Simplify Test**
   - Create minimal reproduction case
   - Test without load balancer
   - Compare with working static endpoint test

### Short Term (This Week)

4. **Fix the Implementation**
   - Implement solution based on debugging findings
   - Options:
     - a) Use `vertx.executeBlocking()` for DNS queries
     - b) Pre-resolve DNS in `EndpointResolvers.dnsSrv()` factory
     - c) Implement async resolution with callback-based API

5. **Validate Fix**
   - Ensure `testJetcdClientWithDnsSrvSingleNode` passes
   - Run test multiple times to check for flakiness
   - Test with multi-node cluster if time permits

6. **Documentation**
   - Update `docs/DNS_SRV_RESOLUTION.md` with working examples
   - Add troubleshooting section
   - Document limitations (e.g., no dynamic re-resolution)

### Medium Term (Next Sprint)

7. **Additional Testing**
   - Multi-node DNS SRV test (currently disabled)
   - Failover scenarios
   - DNS server unavailability handling
   - Invalid SRV record handling

8. **Performance Testing**
   - Measure DNS resolution overhead
   - Test with high-throughput workloads
   - Consider caching resolved addresses

9. **Re-enable Lease Memory Leak Test**
   - Separate from DNS work, but pending
   - Review issue #1236
   - Fix flakiness root cause

## Remaining Disabled Tests

### 1. DNS SRV KV Test (This Work)
**Test**: `testJetcdClientWithDnsSrvSingleNode`  
**Status**: Fails with timeout  
**Blocker**: Yes - blocks DNS SRV feature completion

### 2. Lease Memory Leak Test (Unrelated)
**Test**: `LeaseTest` (specific test TBD)  
**Status**: Flaky  
**Issue**: #1236  
**Blocker**: No - separate issue from DNS work

## Key Files Reference

### Implementation
- `jetcd-core/src/main/java/io/etcd/jetcd/resolver/DnsSrvAddressResolver.java` - Custom resolver
- `jetcd-core/src/main/java/io/etcd/jetcd/resolver/EndpointResolver.java` - Interface
- `jetcd-core/src/main/java/io/etcd/jetcd/resolver/EndpointResolvers.java` - Factory methods
- `jetcd-core/src/main/java/io/etcd/jetcd/resolver/AbstractEndpointResolver.java` - Base class

### Integration Points
- `jetcd-core/src/main/java/io/etcd/jetcd/impl/ClientConnectionManager.java` - Uses resolver
- `jetcd-core/src/main/java/io/etcd/jetcd/impl/KVImpl.java` - KV client (requires SocketAddress cast)
- `jetcd-core/src/main/java/io/etcd/jetcd/impl/AuthImpl.java` - Auth client
- `jetcd-core/src/main/java/io/etcd/jetcd/impl/LeaseImpl.java` - Lease client

### Tests
- `jetcd-core/src/test/java/io/etcd/jetcd/impl/DnsSrvIntegrationTest.java` - Integration tests
- `jetcd-core/src/test/java/io/etcd/jetcd/impl/ClientConnectionManagerTest.java` - API tests

### Documentation
- `docs/migration-to-vertx-grpc-client.md` - Migration plan
- `docs/DNS_SRV_RESOLUTION.md` - Usage documentation (may need updates)

## Recent Commits

1. **refactor(deps): remove unused vertx-service-resolver dependency**
   - Removed dependency from build.gradle and libs.versions.toml
   - Tests confirm no breakage (3/4 pass)
   
2. **feat(resolver): implement per-Vertx caching in DnsSrvAddressResolver**
   - Added ConcurrentHashMap for resolver caching
   - Used computeIfAbsent for thread-safe lazy initialization
   
3. **feat(resolver): implement custom DNS SRV resolver**
   - Created DnsSrvAddressResolver with DnsClient integration
   - Updated EndpointResolver interface to return Address
   - Added SocketAddress casts in implementation classes

4. **test(dns): add DNS SRV integration tests with dnsmasq**
   - Simplified test without multi-node cluster
   - Added jetcd DNS SRV resolver tests
   - Testcontainers-based infrastructure

## Notes for Continuation

### Investigation Hypothesis
The most likely issue is that the blocking `.get()` call in `DnsSrvAddressResolver.createEndpointResolver()` is being invoked from a Vert.x event loop thread, causing a deadlock. The DNS query future can't complete because the event loop is blocked waiting for it.

### Quick Win Option
Instead of fixing the async resolution, consider simpler approach:
```java
// In EndpointResolvers.DnsSrv.create()
public static DnsSrv create(String serviceName, String dnsServer, int dnsPort) {
    // Pre-resolve DNS synchronously in factory method (NOT in resolver)
    List<SocketAddress> addresses = resolveDnsSync(serviceName, dnsServer, dnsPort);
    // Return static resolver with resolved addresses
    return new DnsSrv(StaticAddressResolver.create(addresses), addresses.get(0));
}
```

This sacrifices dynamic re-resolution but solves the immediate blocking issue.

### Long-term Solution
Implement fully async resolution by:
1. Making `endpointResolver()` return a `Future<EndpointResolver>` (API change)
2. Using Vert.x `DnsClient` with `.onComplete()` callbacks
3. Updating all call sites to handle async resolver creation

This is more complex but provides proper async behavior.

---

**Status Summary**: DNS SRV resolution is 90% complete. The resolver implementation works for basic DNS queries but has a critical blocking issue preventing end-to-end KV operations. Once the timeout issue is resolved and tests pass, the feature is ready for production use.

