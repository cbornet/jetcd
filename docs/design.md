# jetcd Design

jetcd is the official Java client for etcd v3, built on Vert.x gRPC with async-first design.

## Architecture

```
Client
  └── GrpcService
        ├── GrpcClient (base)
        ├── GrpcClient (authenticated)
        └── ServiceResolver<SocketAddress>

Client Services: KV, Watch, Lease, Auth, Cluster, Maintenance, Lock, Election
```

## Design Documents

- **[API Design](design/api.md)** - ByteSequence vs String patterns, type safety
- **[Watch](design/watch.md)** - Watch implementation with auto-reconnect
- **[SSL/TLS](design/ssl.md)** - Secure client configuration
- **[DNS SRV](design/dns-srv.md)** - Dynamic service discovery
- **[Load Balancing](design/load-balancing.md)** - Client-side load balancing strategies

## Key Design Decisions

### Async-First with CompletableFuture

All operations return `CompletableFuture` for non-blocking usage. Internal implementation uses Vert.x futures, converted at API boundaries.

### Lazy Initialization

Client services (KV, Watch, etc.) are created on first access via memoizing suppliers, avoiding unnecessary resource allocation.

### Type-Safe Service Resolution

`ServiceResolver<SocketAddress>` provides strongly-typed endpoint resolution with support for static endpoints and DNS SRV discovery.

### Retry with Failsafe

Automatic retry with exponential backoff, auth token refresh on auth errors, configurable retry policies.

