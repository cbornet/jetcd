# Security Integration Tasks

This document outlines the remaining security integration work for the Java 21 modernization effort.

## Context

`SecureByteSequence` has been implemented (in `jetcd-core/src/main/java/io/etcd/jetcd/SecureByteSequence.java`) but is not yet integrated into the authentication system. This would enable automatic zeroing of credentials in memory.

---

## Task 1: ClientBuilder SecureByteSequence Integration

### Objective
Update `ClientBuilder` to accept `SecureByteSequence` for credentials, enabling automatic memory zeroing of sensitive data.

### Files to Modify
- `jetcd-core/src/main/java/io/etcd/jetcd/ClientBuilder.java`

### Current State
```java
public ClientBuilder user(ByteSequence user)
public ClientBuilder password(ByteSequence password)
```

### Required Changes

1. **Add new methods accepting `SecureByteSequence`**:
   ```java
   public ClientBuilder user(SecureByteSequence user) {
       this.user = user;
       return this;
   }
   
   public ClientBuilder password(SecureByteSequence password) {
       this.password = password;
       return this;
   }
   ```

2. **Keep existing `ByteSequence` methods for backward compatibility**:
   - Existing methods should wrap the `ByteSequence` in a `SecureByteSequence`
   - Add deprecation warnings recommending the secure version

3. **Update internal storage**:
   - Change `private ByteSequence user` to `private SecureByteSequence user`
   - Change `private ByteSequence password` to `private SecureByteSequence password`

4. **Ensure proper cleanup**:
   - When `ClientBuilder` creates a `Client`, ensure credentials are zeroed after use
   - Consider adding a `close()` or cleanup mechanism

### Migration Example to Add to Docs
```java
// Before (less secure)
ClientBuilder.builder()
    .user(ByteSequence.from("admin"))
    .password(ByteSequence.from("secret123"))
    .build();

// After (more secure - credentials zeroed after use)
try (SecureByteSequence password = SecureByteSequence.from("secret123")) {
    ClientBuilder.builder()
        .user(SecureByteSequence.from("admin"))
        .password(password)
        .build();
}
```

### Breaking Changes
- `ClientBuilder` internal fields change type (but public API remains backward compatible)

---

## Task 2: GrpcAuth Token Zeroing

### Objective
Ensure authentication tokens in `GrpcAuth` are zeroed when no longer needed, preventing token leakage in memory dumps.

### Files to Modify
- `jetcd-core/src/main/java/io/etcd/jetcd/grpc/GrpcAuth.java`

### Current State
- `GrpcAuth` manages authentication tokens
- Tokens are stored as `String` or `ByteSequence`
- No explicit zeroing mechanism

### Required Changes

1. **Identify token storage locations**:
   - Look for fields storing authentication tokens
   - Check caching mechanisms

2. **Convert token storage to use `SecureByteSequence`**:
   ```java
   // Before
   private volatile String token;
   
   // After
   private volatile SecureByteSequence token;
   ```

3. **Implement zeroing on token refresh**:
   - Before replacing an old token, call `token.close()` to zero it
   - Use try-with-resources where appropriate

4. **Add cleanup on client close**:
   - Implement `AutoCloseable` or add to existing close mechanism
   - Ensure all tokens are zeroed when the auth client is closed

5. **Handle token conversion**:
   - gRPC may require tokens as `String`
   - Convert only when needed: `new String(token.getBytes())`
   - Zero the String immediately after use if possible (note: Strings are immutable, so this is best-effort)

### Key Considerations
- Thread safety: tokens may be accessed concurrently
- Token refresh: old tokens must be zeroed before replacement
- Error handling: tokens should be zeroed even on exceptions

### Example Pattern
```java
public void refreshToken() {
    SecureByteSequence newToken = fetchNewToken();
    
    SecureByteSequence oldToken = this.token;
    this.token = newToken;
    
    // Zero old token
    if (oldToken != null) {
        oldToken.close();
    }
}
```

---

## Task 3: Security Integration Tests

### Objective
Verify that credential zeroing works correctly end-to-end.

### Files to Create
- `jetcd-core/src/test/java/io/etcd/jetcd/ClientBuilderSecurityTest.java`
- `jetcd-core/src/test/java/io/etcd/jetcd/grpc/GrpcAuthSecurityTest.java`

### Test Categories

#### 3.1 ClientBuilder Credential Zeroing Tests

Create `ClientBuilderSecurityTest.java`:

```java
@Test
void testPasswordZeroedAfterClientCreation() {
    byte[] originalPassword = "secret123".getBytes();
    byte[] passwordCopy = Arrays.copyOf(originalPassword, originalPassword.length);
    
    SecureByteSequence password = SecureByteSequence.from(passwordCopy);
    
    // Build client (password should be used)
    Client client = Client.builder()
        .endpoints("http://localhost:2379")
        .user(SecureByteSequence.from("admin"))
        .password(password)
        .build();
    
    // Close password - should zero the underlying bytes
    password.close();
    
    // Verify the bytes are zeroed
    assertThat(passwordCopy).containsOnly((byte) 0);
    
    client.close();
}

@Test
void testMultiplePasswordsOnlyLastIsUsed() {
    // Test that calling password() multiple times properly cleans up old values
}

@Test
void testUserCredentialZeroing() {
    // Similar to password test but for user
}
```

#### 3.2 GrpcAuth Token Zeroing Tests

Create `GrpcAuthSecurityTest.java`:

```java
@Test
void testTokenZeroedOnRefresh() {
    // 1. Authenticate and get initial token
    // 2. Capture reference to token's underlying bytes
    // 3. Trigger token refresh
    // 4. Verify old token bytes are zeroed
}

@Test
void testTokenZeroedOnClose() {
    // 1. Create authenticated GrpcAuth
    // 2. Capture reference to token bytes
    // 3. Close GrpcAuth
    // 4. Verify token bytes are zeroed
}

@Test
void testTokenZeroedOnError() {
    // 1. Simulate error during token refresh
    // 2. Verify old token is still zeroed despite error
}
```

#### 3.3 Integration Tests

Add to existing integration test suite:

```java
@Test
void testSecureCredentialsEndToEnd() {
    // This requires a real etcd instance with auth enabled
    // 1. Create client with SecureByteSequence credentials
    // 2. Perform authenticated operations
    // 3. Close client
    // 4. Verify credentials were zeroed
}

@Test
void testMemoryDoesNotLeakCredentials() {
    // Best-effort test to verify credentials don't appear in heap dumps
    // Note: This is difficult to test reliably but provides some confidence
}
```

### Test Utilities

Consider creating a helper class:

```java
public class SecurityTestUtils {
    /**
     * Captures a reference to the underlying byte array of a SecureByteSequence.
     * Useful for testing zeroing behavior.
     */
    public static byte[] captureUnderlyingBytes(SecureByteSequence seq) {
        // Use reflection to access private bytes field
        // Return reference (not copy) so we can verify it's zeroed
    }
    
    /**
     * Verifies that a byte array has been zeroed.
     */
    public static void assertZeroed(byte[] bytes) {
        assertThat(bytes).containsOnly((byte) 0);
    }
}
```

### Testing Challenges

1. **Strings are immutable**: Java Strings cannot be zeroed. Focus testing on byte arrays.
2. **Reflection needed**: May need reflection to access private fields for verification.
3. **Requires auth-enabled etcd**: Some tests need a real etcd instance with authentication.
4. **JVM optimizations**: The JVM may optimize away or duplicate sensitive data. Tests are best-effort.

### Test Execution

Run tests with:
```bash
./gradlew :jetcd-core:test --tests "*SecurityTest"
```

---

## Implementation Order

1. **Start with Task 1** (ClientBuilder) - most straightforward
2. **Then Task 2** (GrpcAuth) - builds on ClientBuilder changes
3. **Finally Task 3** (Tests) - validates both implementations

## Documentation Updates

After implementation, update `docs/JAVA21_MIGRATION.md`:

1. Add `ClientBuilder` secure credential usage examples
2. Document security guarantees and limitations
3. Add "Security Best Practices" section
4. Update breaking changes table

## Breaking Changes

- **ClientBuilder**: Internal fields change type (backward compatible API)
- **GrpcAuth**: Internal implementation changes (no public API changes)
- **Security guarantees**: Document best-effort nature of memory zeroing

## Success Criteria

- [ ] ClientBuilder accepts `SecureByteSequence` credentials
- [ ] Deprecated `ByteSequence` methods remain for compatibility
- [ ] GrpcAuth tokens are zeroed on refresh and close
- [ ] All security tests pass
- [ ] No memory leaks in long-running auth scenarios
- [ ] Documentation updated with security best practices
- [ ] Existing tests continue to pass

---

## References

- `SecureByteSequence.java` - Base implementation
- `SecureByteSequenceTest.java` - Example test patterns
- `ClientBuilder.java` - Current authentication API
- `GrpcAuth.java` - Current token management

