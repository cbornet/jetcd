# Dynamic Endpoint Resolution Fix - Implementation Summary

## Problem Solved

Tests were failing with "Connection refused" errors when the `testKVClientCanRetryPutOnEtcdRestart` test restarted the etcd cluster. The cluster restart caused Testcontainers to assign new random ports, but clients continued using stale endpoints cached at client creation time.

## Root Cause

The `EtcdClusterEndpointResolver.create()` method was capturing cluster endpoints into a static list at creation time:

```java
// OLD CODE - Static endpoint capture
List<SocketAddress> addresses = cluster.containers().stream()
    .map(EtcdContainer::getClientAddress)
    .map(addr -> SocketAddress.inetSocketAddress(addr.getPort(), addr.getHostName()))
    .collect(Collectors.toList());

AddressResolver resolver = AddressResolver.mappingResolver(ignored -> addresses);
```

This meant:
1. Client created in `@BeforeAll` with endpoints (e.g., `localhost:33418`)
2. `testKVClientCanRetryPutOnEtcdRestart` restarts cluster
3. Cluster gets new ports (e.g., `localhost:33430`)
4. Client still tries to connect to old port (33418)
5. **Connection refused!**

## Solution Implemented

Modified `EtcdClusterEndpointResolver.create()` to query endpoints dynamically on each resolution attempt:

```java
// NEW CODE - Dynamic endpoint resolution
AddressResolver resolver = AddressResolver.mappingResolver(ignored -> {
    return cluster.containers().stream()
        .map(EtcdContainer::getClientAddress)
        .map(addr -> SocketAddress.inetSocketAddress(addr.getPort(), addr.getHostName()))
        .collect(Collectors.toList());
});
```

**Key Change**: Moved the stream operation inside the lambda so it executes on **each resolution**, not just once at creation time.

## Files Modified

### 1. `/jetcd-test/src/main/java/io/etcd/jetcd/test/EtcdClusterEndpointResolver.java`

**Change**: Modified `create()` method to use dynamic endpoint resolution
- Lambda now queries fresh endpoints on each resolution attempt
- Adapts automatically to cluster restarts and port changes
- Added documentation explaining the dynamic behavior

### 2. `/jetcd-core/src/test/java/io/etcd/jetcd/impl/KVTest.java`

**Change**: Removed diagnostic logging code
- Removed `TestInfo` import
- Removed `logTestStart()` method
- Removed `logTestEnd()` method
- Removed `diagnosticTestEndpoints()` test
- Removed `testContainerLifecycle()` test
- Cleaned up `setUp()` method

## Test Results

**Before Fix**:
```
22 tests completed, 10 failed
- Connection refused: localhost/127.0.0.1:33418
```

**After Fix** (3 consecutive runs):
```
Run 1: 22 passing (18.5s) ✓
Run 2: 22 passing (18.7s) ✓
Run 3: 22 passing (18.9s) ✓
```

All tests now pass consistently, including:
- ✅ `testKVClientCanRetryPutOnEtcdRestart` (restarts cluster mid-test)
- ✅ All subsequent tests continue working with restarted cluster
- ✅ No more "Connection refused" errors

## Benefits

1. **Robustness**: Clients automatically adapt to cluster restarts
2. **Test Reliability**: Tests no longer fail due to stale endpoints
3. **Clean Code**: No workarounds or special handling needed
4. **Future-Proof**: Works for any cluster lifecycle changes

## Technical Details

### How Dynamic Resolution Works

1. **Client Creation**: `TestUtil.client(cluster).build()` creates client with resolver
2. **Connection Attempt**: Vert.x gRPC tries to connect
3. **Endpoint Resolution**: Resolver lambda executes
4. **Fresh Query**: `cluster.containers().map(getClientAddress)` gets current addresses
5. **Connection**: Client connects to current endpoint

### Why This is Safe

- `cluster.containers()` is a live query, not a cached list
- `getClientAddress()` returns current container address
- Testcontainers ensures containers remain accessible
- No performance impact (resolution is infrequent, connections are reused)

## Investigation Documents

The following documents detail the investigation process:
- `ROOT_CAUSE_FOUND.md` - Initial discovery of port changes
- `ENDPOINT_RESOLUTION_ANALYSIS.md` - Analysis of endpoint caching
- `FINAL_ROOT_CAUSE_ANALYSIS.md` - Complete root cause with solutions
- `KV_INFRASTRUCTURE_ANALYSIS.md` - Earlier infrastructure analysis
- `KV_TEST_ANALYSIS.md` - Test failure patterns

## Conclusion

The dynamic endpoint resolution fix solves the core issue of stale endpoint caching. Tests now survive cluster restarts seamlessly, and the solution is clean, maintainable, and robust.

