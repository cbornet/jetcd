# jetcd API Design Patterns

This document explains key design decisions in the jetcd API, particularly around type usage and consistency patterns.

## Table of Contents

- [ByteSequence vs String Usage](#bytesequence-vs-string-usage)
- [Design Principles](#design-principles)
- [Usage Guidelines](#usage-guidelines)
- [Examples](#examples)
- [Migration Guide](#migration-guide)

---

## ByteSequence vs String Usage

### Overview

The jetcd API uses two primary types for text/binary data: `ByteSequence` and `String`. This is **intentional and semantically meaningful**, not an inconsistency.

**TL;DR:**
- **Data Operations** (keys, values, user/role names) → `ByteSequence`
- **Infrastructure/Configuration** (endpoints, DNS names, headers) → `String`

### Why Two Types?

#### ByteSequence: For etcd Data

`ByteSequence` is used for all data stored in or retrieved from etcd:

```java
public final class ByteSequence {
    // Wraps com.google.protobuf.ByteString internally
}
```

**Key Benefits:**

1. **Binary Safety**: etcd keys and values can contain any byte sequence, not just valid UTF-8 strings
   ```java
   // Valid etcd key with binary data
   ByteSequence key = ByteSequence.from(new byte[]{0x00, 0x01, 0xFF});
   ```

2. **Encoding Control**: Explicit charset handling avoids platform-dependent defaults
   ```java
   // Explicit encoding
   ByteSequence key = ByteSequence.from("key", StandardCharsets.UTF_8);
   ```

3. **Performance**: 
   - Pre-computed hash codes for efficient map lookups
   - Zero-copy integration with gRPC protobuf (`ByteString`)
   - Efficient prefix/concatenation operations

4. **Immutability**: Thread-safe by design, safe to share across threads

5. **Type Safety**: Prevents accidental string encoding issues

#### String: For Configuration and Infrastructure

`String` is used for configuration parameters that are inherently textual:

```java
// Network endpoints are URLs, not etcd keys
CompletableFuture<StatusResponse> statusMember(String target);

// DNS service names are text
ServiceResolver dnsSrv(String serviceName);

// HTTP headers are defined as strings by spec
ClientBuilder header(String key, String value);
```

**Rationale:**

1. **Semantic Correctness**: Network addresses, DNS names, and HTTP headers are defined by their respective specifications as text strings
2. **Ergonomics**: No need for unnecessary `ByteSequence.from()` conversions for simple configuration
3. **Standards Compliance**: HTTP headers, URLs, and DNS names are string-based by design
4. **Developer Experience**: Configuration should be simple and readable

---

## Design Principles

### 1. Data vs Configuration Separation

**Data Operations** (stored in etcd):
```java
// KV operations
kv.put(ByteSequence key, ByteSequence value)
kv.get(ByteSequence key)
kv.delete(ByteSequence key)

// Auth operations
auth.userAdd(ByteSequence user, ByteSequence password)
auth.roleAdd(ByteSequence role)

// Watch operations
watch.watch(ByteSequence key, ...)

// Lock operations  
lock.lock(ByteSequence name, long leaseId)

// Election operations
election.campaign(ByteSequence electionName, ...)
```

**Configuration/Infrastructure** (not stored in etcd):
```java
// Cluster member endpoints
maintenance.statusMember(String target)
maintenance.defragmentMember(String target)

// Service discovery
ServiceResolvers.endpoints(String... addresses)
ServiceResolvers.dnsSrv(String serviceName)

// HTTP configuration
clientBuilder.header(String key, String value)
```

### 2. Explicit > Implicit

The API favors explicit encoding decisions:

```java
// GOOD: Explicit encoding visible
ByteSequence key = ByteSequence.from("mykey", StandardCharsets.UTF_8);
kv.put(key, value);

// AVOIDED: Would hide encoding decision
kv.put("mykey", "myvalue"); // Which charset? UTF-8? Platform default?
```

### 3. Binary-First Design

etcd is a binary key-value store. The API reflects this reality:

```java
// etcd stores bytes, not strings
ByteSequence binaryKey = ByteSequence.from(new byte[]{0x01, 0x02, 0x03});
ByteSequence binaryValue = ByteSequence.from(protobufMessage.toByteArray());
kv.put(binaryKey, binaryValue);
```

### 4. Type Safety and Correctness

Using distinct types prevents category errors:

```java
// GOOD: Type system prevents mistakes
ByteSequence key = ByteSequence.from("key");
String endpoint = "http://localhost:2379";
kv.get(key); // Correct
maintenance.statusMember(endpoint); // Correct

// Would be BAD if API used ByteSequence everywhere:
kv.get(endpoint); // Compile error - endpoint is not a key!
```

---

## Usage Guidelines

### When to Use ByteSequence

Use `ByteSequence` for:

✅ **etcd Keys**
```java
ByteSequence key = ByteSequence.from("/config/database/host");
```

✅ **etcd Values**
```java
ByteSequence value = ByteSequence.from("localhost:5432");
```

✅ **User and Role Names**
```java
auth.userAdd(ByteSequence.from("alice"), ByteSequence.from("password123"));
auth.roleAdd(ByteSequence.from("admin"));
```

✅ **Lock Names**
```java
lock.lock(ByteSequence.from("/locks/resource-1"), leaseId);
```

✅ **Election Names and Proposals**
```java
election.campaign(ByteSequence.from("/elections/leader"), leaseId, 
                 ByteSequence.from("node-1"));
```

✅ **Any Binary Data**
```java
// Protocol buffer messages
ByteSequence value = ByteSequence.from(myProto.toByteArray());

// Encrypted data
ByteSequence encrypted = ByteSequence.from(cipher.doFinal(plaintext));
```

### When to Use String

Use `String` for:

✅ **Cluster Member Endpoints**
```java
maintenance.statusMember("http://etcd-1:2379");
maintenance.defragmentMember("http://etcd-2:2379");
```

✅ **Service Discovery Configuration**
```java
ServiceResolver resolver = ServiceResolvers.endpoints(
    "http://etcd-1:2379",
    "http://etcd-2:2379"
);

ServiceResolver dnsSrv = ServiceResolvers.dnsSrv("_etcd._tcp.example.com");
```

✅ **HTTP Headers**
```java
Client client = Client.builder(endpoints)
    .header("X-Custom-Header", "value")
    .build();
```

✅ **DNS Configuration**
```java
ServiceResolvers.dnsSrv("etcd-service", "dns.example.com", 53);
```

### Conversion Utilities

`ByteSequence` provides convenient factory methods:

```java
// From String with default UTF-8 encoding
ByteSequence key = ByteSequence.from("mykey");

// From String with explicit encoding
ByteSequence key = ByteSequence.from("mykey", StandardCharsets.UTF_8);

// From byte array
ByteSequence key = ByteSequence.from(new byte[]{1, 2, 3});

// From ByteString (protobuf)
ByteSequence key = ByteSequence.from(byteString);

// Empty sequence
ByteSequence empty = ByteSequence.EMPTY;
```

Converting back:

```java
// To String with explicit encoding
String str = key.toString(StandardCharsets.UTF_8);

// To byte array
byte[] bytes = key.getBytes();
```

---

## Examples

### Example 1: KV Operations (ByteSequence)

```java
Client client = Client.builder("http://localhost:2379").build();
KV kv = client.getKVClient();

// Put a key-value pair
ByteSequence key = ByteSequence.from("/config/timeout");
ByteSequence value = ByteSequence.from("30");
kv.put(key, value).get();

// Get the value
GetResponse response = kv.get(key).get();
if (response.getCount() > 0) {
    ByteSequence retrieved = response.getKvs().get(0).getValue();
    String timeoutStr = retrieved.toString(StandardCharsets.UTF_8);
    System.out.println("Timeout: " + timeoutStr);
}

// Delete the key
kv.delete(key).get();
```

### Example 2: Maintenance Operations (String)

```java
Client client = Client.builder(
    "http://etcd-1:2379",
    "http://etcd-2:2379",
    "http://etcd-3:2379"
).build();

Maintenance maintenance = client.getMaintenanceClient();

// Check status of specific members (String endpoints)
StatusResponse status1 = maintenance.statusMember("http://etcd-1:2379").get();
StatusResponse status2 = maintenance.statusMember("http://etcd-2:2379").get();

System.out.println("Member 1 version: " + status1.getVersion());
System.out.println("Member 2 version: " + status2.getVersion());

// Defragment specific member (String endpoint)
maintenance.defragmentMember("http://etcd-3:2379").get();
```

### Example 3: Mixed Usage (Both Types)

```java
// Configuration uses String
Client client = Client.builder("http://localhost:2379")
    .header("X-Application", "MyApp")  // String for HTTP header
    .build();

// Data operations use ByteSequence
KV kv = client.getKVClient();
Auth auth = client.getAuthClient();

// Create admin user (ByteSequence for etcd data)
ByteSequence username = ByteSequence.from("admin");
ByteSequence password = ByteSequence.from("secret123");
auth.userAdd(username, password).get();

// Store user preferences (ByteSequence for etcd data)
ByteSequence prefKey = ByteSequence.from("/users/admin/preferences");
ByteSequence prefValue = ByteSequence.from("{\"theme\":\"dark\"}");
kv.put(prefKey, prefValue).get();

// Check cluster member status (String for endpoint)
Maintenance maintenance = client.getMaintenanceClient();
maintenance.statusMember("http://localhost:2379").get();
```

### Example 4: Binary Data (ByteSequence)

```java
// Storing protocol buffer messages
MyProtoMessage message = MyProtoMessage.newBuilder()
    .setField1("value1")
    .setField2(42)
    .build();

ByteSequence key = ByteSequence.from("/data/proto/message1");
ByteSequence value = ByteSequence.from(message.toByteArray());
kv.put(key, value).get();

// Retrieving and deserializing
GetResponse response = kv.get(key).get();
byte[] serialized = response.getKvs().get(0).getValue().getBytes();
MyProtoMessage retrieved = MyProtoMessage.parseFrom(serialized);
```

### Example 5: Service Resolution (String)

```java
// Static endpoints configuration (String)
ServiceResolver staticResolver = ServiceResolvers.endpoints(
    "http://etcd-1.example.com:2379",
    "http://etcd-2.example.com:2379"
);

// DNS SRV configuration (String)
ServiceResolver dnsResolver = ServiceResolvers.dnsSrv(
    "_etcd-client._tcp.example.com",
    "dns.example.com",
    53
);

// Both resolve to endpoints, but configuration uses String
Client client = Client.builder(dnsResolver).build();

// Data operations still use ByteSequence
KV kv = client.getKVClient();
kv.put(ByteSequence.from("key"), ByteSequence.from("value")).get();
```

---

## Anti-Patterns to Avoid

### ❌ Don't Use String Everywhere

```java
// BAD: This design would be incorrect
interface KV {
    CompletableFuture<PutResponse> put(String key, String value);
}

// Problems:
// 1. Assumes UTF-8 encoding (implicit decision)
// 2. Can't store binary data (protocol buffers, encrypted data)
// 3. Performance overhead (string encoding/decoding on every operation)
// 4. Loses type safety (keys vs endpoints confusion)
```

### ❌ Don't Use ByteSequence for Configuration

```java
// BAD: This would be unnecessarily verbose
ServiceResolver resolver = ServiceResolvers.endpoints(
    ByteSequence.from("http://localhost:2379")  // Unnecessary
);

maintenance.statusMember(ByteSequence.from("http://localhost:2379"));  // Awkward
```

### ❌ Don't Mix Concerns

```java
// BAD: Using data type for configuration
ByteSequence endpoint = ByteSequence.from("http://localhost:2379");
// endpoint is configuration, not etcd data

// GOOD: Clear distinction
String endpoint = "http://localhost:2379";  // Configuration
ByteSequence key = ByteSequence.from("/app/config");  // Data
```

---

## Migration Guide

### If You're Coming From jetcd 0.5.x or Earlier

Older versions may have used `String` in some places. Here's how to migrate:

```java
// Old (hypothetical)
kv.put("mykey", "myvalue");

// New (current API)
kv.put(ByteSequence.from("mykey"), ByteSequence.from("myvalue"));
```

**Why the Change?**

1. **Binary Safety**: Original design couldn't handle binary data
2. **Performance**: String conversion on every operation was costly
3. **Correctness**: Explicit encoding prevents bugs
4. **Standards**: Aligns with etcd's binary nature

### Adding Convenience Wrappers (If Needed)

If your application only uses UTF-8 strings, you can create convenience wrappers:

```java
public class StringKV {
    private final KV kv;
    
    public StringKV(KV kv) {
        this.kv = kv;
    }
    
    public CompletableFuture<String> put(String key, String value) {
        return kv.put(ByteSequence.from(key), ByteSequence.from(value))
            .thenApply(response -> value);
    }
    
    public CompletableFuture<Optional<String>> get(String key) {
        return kv.get(ByteSequence.from(key))
            .thenApply(response -> {
                if (response.getCount() == 0) {
                    return Optional.empty();
                }
                return Optional.of(
                    response.getKvs().get(0).getValue()
                        .toString(StandardCharsets.UTF_8)
                );
            });
    }
}

// Usage
StringKV stringKV = new StringKV(client.getKVClient());
stringKV.put("key", "value").get();
Optional<String> value = stringKV.get("key").get();
```

**Note**: This wrapper is **not** included in jetcd core because:
- It hides encoding decisions
- It prevents binary data usage
- It's trivial to implement when needed
- It doesn't fit all use cases

---

## Design History

### Why Not Just Use `byte[]`?

While `byte[]` is simpler, it has drawbacks:

```java
// Problems with byte[]:
byte[] key = "mykey".getBytes();

// 1. Mutable - not thread-safe
key[0] = 'x';  // Oops, modified the "immutable" key

// 2. No efficient hash code caching
// (arrays use identity-based hashCode, not content-based)

// 3. No efficient protobuf integration
// (requires copying for every gRPC call)

// 4. No utility methods (concat, startsWith, substring)
```

`ByteSequence` solves all these problems while maintaining efficiency.

### Why Not Just Use `String` Everywhere?

```java
// Problems with String everywhere:

// 1. Can't store binary data
byte[] encrypted = cipher.doFinal(plaintext);
String key = new String(encrypted);  // WRONG! Lossy conversion

// 2. Encoding ambiguity
String value = "café";
byte[] bytes = value.getBytes();  // Which charset? Platform default?

// 3. Performance overhead
// Every operation requires string encoding/decoding

// 4. Not semantically correct
// etcd is a binary store, not a text database
```

### Evolution of the API

1. **Early versions**: Mixed `String` and `byte[]` usage - inconsistent
2. **v0.3.0**: Introduced `ByteSequence` for type safety and performance
3. **v0.5.0**: Standardized on `ByteSequence` for all etcd data operations
4. **v0.7.0**: Maintained `String` for infrastructure (maintenance operations)
5. **Current**: Clear separation between data (`ByteSequence`) and configuration (`String`)

---

## Related Documentation

- **Watch Operations**: See [Watch.md](Watch.md) for watch-specific patterns
- **Security**: See [SslConfig.md](SslConfig.md) for TLS configuration (uses `String` for paths)
- **Service Discovery**: See [DNS_SRV_RESOLUTION.md](DNS_SRV_RESOLUTION.md) for DNS configuration (uses `String`)

---

## Summary

| Operation Type | Parameter Type | Rationale |
|---------------|----------------|-----------|
| **KV operations** | `ByteSequence` | Binary data stored in etcd |
| **Auth operations** | `ByteSequence` | User/role names stored in etcd |
| **Watch operations** | `ByteSequence` | Keys stored in etcd |
| **Lock operations** | `ByteSequence` | Lock names stored in etcd |
| **Election operations** | `ByteSequence` | Election data stored in etcd |
| **Maintenance operations** | `String` | Network endpoints (not etcd data) |
| **Service resolution** | `String` | URLs and DNS names (not etcd data) |
| **HTTP headers** | `String` | HTTP spec defines headers as strings |

**Golden Rule**: If it's stored in etcd or represents etcd data, use `ByteSequence`. If it's configuration or infrastructure, use `String`.

---

## Contributing

When adding new API methods, follow these guidelines:

1. **Data Operations**: Use `ByteSequence` for keys, values, and any data stored in etcd
2. **Configuration**: Use `String` for URLs, endpoints, DNS names, and HTTP headers
3. **Type Safety**: Don't create overloads that hide encoding decisions
4. **Documentation**: Explain why the type was chosen in JavaDoc

Questions about API design? Open a GitHub issue with the `api-design` label.

---

**Document Version**: 1.0  
**Last Updated**: 2025-12-22  
**Status**: Active

