# Java 21 Modernization Migration Guide

This guide covers the breaking changes introduced in the Java 21 modernization effort.

## Overview

The following Java 21 features have been introduced:
- **Records** for immutable data classes
- **Sealed Interfaces** for state hierarchies  
- **SecureByteSequence** for secure credential handling
- **Optional Return Values** for explicit absence handling

## Records Migration

### Breaking Change: Accessor Method Names

Classes converted to records have accessor methods without the `get` prefix.

#### LeaderKey

```java
// Before
LeaderKey key = ...;
ByteSequence name = key.getName();
ByteSequence keyBytes = key.getKey();
long revision = key.getRevision();
long lease = key.getLease();

// After
LeaderKey key = ...;
ByteSequence name = key.name();
ByteSequence keyBytes = key.key();
long revision = key.revision();
long lease = key.lease();
```

####AlarmMember

```java
// Before
AlarmMember member = ...;
long id = member.getMemberId();
AlarmType type = member.getAlarmType();

// After
AlarmMember member = ...;
long id = member.memberId();
AlarmType type = member.alarmType();
```

#### WatchEvent

```java
// Before
WatchEvent event = ...;
KeyValue kv = event.getKeyValue();
KeyValue prevKv = event.getPrevKV();
EventType type = event.getEventType();

// After
WatchEvent event = ...;
KeyValue kv = event.keyValue();
KeyValue prevKv = event.prevKV();
EventType type = event.eventType();
```

### Migration Strategy

**Find and Replace Patterns**:
```bash
# LeaderKey
find: \.getName\(\)
replace: .name()

find: \.getKey\(\)
replace: .key()

find: \.getRevision\(\)
replace: .revision()

find: \.getLease\(\)
replace: .lease()

# AlarmMember
find: \.getMemberId\(\)
replace: .memberId()

find: \.getAlarmType\(\)
replace: .alarmType()

# WatchEvent
find: \.getKeyValue\(\)
replace: .keyValue()

find: \.getPrevKV\(\)
replace: .prevKV()

find: \.getEventType\(\)
replace: .eventType()
```

## Sealed Interfaces

### WatchState

WatchState is now a sealed interface with nested record implementations. This change is **backward compatible** for most use cases.

```java
// Before (still works)
WatchState state = WatchState.WATCHING;

if (state == WatchState.WATCHING) {
    // ...
}

// After (new pattern matching capability)
String description = switch (state) {
    case WatchState.Connecting() -> "Connecting...";
    case WatchState.Subscribing() -> "Subscribing...";
    case WatchState.Watching() -> "Watching";
    case WatchState.Reconnecting() -> "Reconnecting...";
    case WatchState.Closed() -> "Closed";
};
```

**Benefits**:
- Compiler enforces exhaustive handling
- Enables state-specific data in the future
- Better IDE autocomplete

## SecureByteSequence

### Overview

`SecureByteSequence` provides secure handling of sensitive data (passwords, tokens) by zeroing memory when closed.

### Usage

```java
// Recommended: try-with-resources
try (SecureByteSequence password = SecureByteSequence.from("myPassword")) {
    Client client = Client.builder("localhost:2379")
        .user(ByteSequence.from("root"))
        .password(password.toByteSequence())
        .build();
} // password automatically zeroed

// From char array (e.g., Console.readPassword())
char[] passwordChars = console.readPassword("Password: ");
try (SecureByteSequence password = SecureByteSequence.from(passwordChars)) {
    Arrays.fill(passwordChars, '\0'); // Zero the char array
    // Use password...
}

// From byte array
byte[] tokenBytes = getTokenFromSecureSource();
try (SecureByteSequence token = SecureByteSequence.from(tokenBytes)) {
    // Use token...
}
```

### Important Notes

1. **Always use try-with-resources** to ensure automatic cleanup
2. **Don't store getBytes() result** - defeats the security purpose
3. **Not foolproof** - provides defense-in-depth, not absolute protection
4. **Thread safety** - don't share across threads without synchronization

### Limitations

- Data may linger until GC runs (minimize window)
- String literals are interned and can't be zeroed
- JVM optimizations may affect zeroing
- Data may be paged to disk (use encrypted swap)
- Heap dumps may capture data before zeroing

### Best Practices

```java
// ✅ Good: Use try-with-resources
try (SecureByteSequence pwd = SecureByteSequence.from(password)) {
    authenticate(pwd);
}

// ✅ Good: Zero char arrays immediately
char[] chars = console.readPassword();
try (SecureByteSequence pwd = SecureByteSequence.from(chars)) {
    Arrays.fill(chars, '\0');
    authenticate(pwd);
}

// ❌ Bad: Storing bytes defeats security
SecureByteSequence pwd = SecureByteSequence.from("pass");
byte[] bytes = pwd.getBytes(); // Now bytes won't be zeroed
pwd.close();

// ❌ Bad: Not using try-with-resources
SecureByteSequence pwd = SecureByteSequence.from("pass");
authenticate(pwd);
pwd.close(); // Easy to forget, may leak if exception thrown
```

## Optional Return Values

### Breaking Change: Nullable Returns → Optional<T>

Methods that may return null now return `Optional<T>` to make absence explicit.

#### PutResponse.getPrevKv()

```java
// Before
PutResponse response = kvClient.put(key, value, 
    PutOption.builder().withPrevKV().build()).get();
    
if (response.hasPrevKv()) {
    KeyValue prev = response.getPrevKv();
    System.out.println("Previous: " + prev.getValue());
}

// After
PutResponse response = kvClient.put(key, value, 
    PutOption.builder().withPrevKV().build()).get();
    
// Functional style (recommended)
response.getPrevKv().ifPresent(prev -> 
    System.out.println("Previous: " + prev.getValue())
);

// Or traditional style
Optional<KeyValue> prevOpt = response.getPrevKv();
if (prevOpt.isPresent()) {
    KeyValue prev = prevOpt.get();
    System.out.println("Previous: " + prev.getValue());
}

// Or with orElse
KeyValue prev = response.getPrevKv().orElse(null);
```

**Migration Note**: The `hasPrevKv()` method is deprecated but still available for transition.

**Benefits**:
- Compiler enforces handling of absent values (no more NPE)
- Explicit in the type system - `Optional<KeyValue>` clearly communicates "may be absent"
- Enables functional programming patterns (`.map()`, `.flatMap()`, `.filter()`, `.orElse()`)

#### Analysis Summary

The codebase analysis identified the following Optional<T> status:

**✅ Already using Optional<T>**:
- `GetOption.getEndKey()` → `Optional<ByteSequence>`
- `DeleteOption.getEndKey()` → `Optional<ByteSequence>`
- `WatchOption.getEndKey()` → `Optional<ByteSequence>`

**✅ Converted to Optional<T>**:
- `PutResponse.getPrevKv()` → `Optional<KeyValue>`

**🗑️ Removed (unused)**:
- `WatchResponseWithError` - This class had no usage in the codebase and has been removed.

**No candidates found**: All response getters return either primitives, non-null collections (Lists never null, may be empty), or already use Optional<T>.

## Summary of Breaking Changes

| Class | Old Method | New Method | Type |
|-------|-----------|------------|------|
| LeaderKey | `getName()` | `name()` | Record accessor |
| LeaderKey | `getKey()` | `key()` | Record accessor |
| LeaderKey | `getRevision()` | `revision()` | Record accessor |
| LeaderKey | `getLease()` | `lease()` | Record accessor |
| AlarmMember | `getMemberId()` | `memberId()` | Record accessor |
| AlarmMember | `getAlarmType()` | `alarmType()` | Record accessor |
| WatchEvent | `getKeyValue()` | `keyValue()` | Record accessor |
| WatchEvent | `getPrevKV()` | `prevKV()` | Record accessor |
| WatchEvent | `getEventType()` | `eventType()` | Record accessor |
| WatchState | `enum` | `sealed interface` | Backward compatible |
| PutResponse | `getPrevKv()` returns `KeyValue` | `getPrevKv()` returns `Optional<KeyValue>` | Optional return value |

## Testing

All existing tests have been updated and pass. Key test areas:
- Election tests (LeaderKey)
- Maintenance tests (AlarmMember)
- Watch tests (WatchEvent, WatchState)
- SecureByteSequence unit tests (15 tests)

## Compatibility

- **Minimum Java Version**: Java 21 (records and sealed interfaces)
- **Source Compatibility**: Breaking changes in method names
- **Binary Compatibility**: Broken (recompilation required)
- **Semantic Compatibility**: Maintained (behavior unchanged)

