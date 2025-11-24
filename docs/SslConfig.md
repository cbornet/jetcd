# How to Build jetcd Client for a TLS-Secured Etcd Cluster

## Prepare Certificate Files

If your etcd cluster is installed using [etcdadm](https://github.com/kubernetes-sigs/etcdadm), you are likely to find
certificate files in the path `/etc/etcd/pki/`: `ca.crt`, `etcdctl-etcd-client.key`, `etcdctl-etcd-client.crt`.

If your etcd cluster is the built-in etcd in a Kubernetes cluster
(using [kubeadm](https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/setup-ha-etcd-with-kubeadm/)),
you can find the same files in `/etc/kubernetes/pki/etcd/`: `ca.crt`, `healthcheck-client.crt`, `healthcheck-client.key`.

## Build jetcd Client with SSL/TLS

jetcd uses Vert.x's `HttpClientOptions` for SSL/TLS configuration, which supports PEM-format certificates. The `SslUtil` class provides convenient helper functions that return `Consumer<HttpClientOptions>` for easy configuration.

### Simple SSL Configuration with CA Trust (Recommended)

For basic SSL configuration where you only need to trust a custom CA certificate, use `SslUtil.withTrustManager()`:

```java
import io.etcd.jetcd.Client;
import io.etcd.jetcd.support.SslUtil;
import java.io.File;

File caCert = new File("/etc/etcd/pki/ca.crt");

Client client = Client.builder()
    .endpoints("https://10.168.168.66:2379")
    .httpClientOptions(SslUtil.withTrustManager(caCert))
    .build();

client.getClusterClient().listMember().get().getMembers().forEach(member -> {
    System.out.println("member: " + member);
});
```

You can also use a file path directly:

```java
import io.etcd.jetcd.support.SslUtil;

Client client = Client.builder()
    .endpoints("https://10.168.168.66:2379")
    .httpClientOptions(SslUtil.withTrustManager("/etc/etcd/pki/ca.crt"))
    .build();
```

Or load from classpath using InputStream:

```java
import io.etcd.jetcd.support.SslUtil;

try (InputStream is = getClass().getResourceAsStream("/ssl/cert/ca.pem")) {
    Client client = Client.builder()
        .endpoints("https://10.168.168.66:2379")
        .httpClientOptions(SslUtil.withTrustManager(is))
        .build();
}
```

### SSL Configuration with Client Certificate Authentication (mTLS)

For mTLS (mutual TLS) where the client needs to present its own certificate, use `SslUtil.withTrustAndKeyManager()`:

```java
import io.etcd.jetcd.Client;
import io.etcd.jetcd.support.SslUtil;
import java.io.File;

File caCert = new File("/etc/etcd/pki/ca.crt");
File clientCert = new File("/etc/etcd/pki/etcdctl-etcd-client.crt");
File clientKey = new File("/etc/etcd/pki/etcdctl-etcd-client.key");

Client client = Client.builder()
    .endpoints("https://10.168.168.66:2379")
    .httpClientOptions(SslUtil.withTrustAndKeyManager(caCert, clientCert, clientKey))
    .build();
```

Or using file paths directly:

```java
import io.etcd.jetcd.support.SslUtil;

Client client = Client.builder()
    .endpoints("https://10.168.168.66:2379")
    .httpClientOptions(SslUtil.withTrustAndKeyManager(
        "/etc/etcd/pki/ca.crt",
        "/etc/etcd/pki/etcdctl-etcd-client.crt",
        "/etc/etcd/pki/etcdctl-etcd-client.key"))
    .build();
```

### Advanced SSL Configuration with Consumer Composition

The `SslUtil` helper methods return `Consumer<HttpClientOptions>`, which can be composed using `.andThen()` for elegant configuration chaining:

```java
import io.etcd.jetcd.Client;
import io.etcd.jetcd.support.SslUtil;

Client client = Client.builder()
    .endpoints("https://10.168.168.66:2379")
    .httpClientOptions(SslUtil.withTrustManager("/path/to/ca.crt")
        .andThen(options -> options
            .setConnectTimeout(5000)
            .setIdleTimeout(60)))
    .build();
```

For maximum control, you can directly configure `HttpClientOptions`:

```java
import io.etcd.jetcd.Client;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.net.PemTrustOptions;

HttpClientOptions httpOptions = new HttpClientOptions()
    .setSsl(true)
    .setUseAlpn(true)
    .setTrustOptions(new PemTrustOptions()
        .addCertPath("/path/to/ca.crt"))
    .setVerifyHost(true)  // Enable hostname verification (default)
    .setTrustAll(false);  // Don't trust all certificates (default)

Client client = Client.builder()
    .endpoints("https://10.168.168.66:2379")
    .httpClientOptions(httpOptions)
    .build();
```

### Disabling Hostname Verification (Not Recommended for Production)

For testing with self-signed certificates that don't match the hostname, you can disable hostname verification using Consumer composition:

```java
import io.etcd.jetcd.Client;
import io.etcd.jetcd.support.SslUtil;

Client client = Client.builder()
    .endpoints("https://localhost:2379")
    .httpClientOptions(SslUtil.withTrustManager("/path/to/ca.crt")
        .andThen(options -> options.setVerifyHost(false)))
    .build();
```

Or to trust all certificates (even less secure):

```java
Client client = Client.builder()
    .endpoints("https://localhost:2379")
    .httpClientOptions(options -> options
        .setSsl(true)
        .setUseAlpn(true)
        .setTrustAll(true)
        .setVerifyHost(false))
    .build();
```

**Note:** Disabling hostname verification and trusting all certificates should only be used in development/testing environments. Always use proper certificate verification in production.

## Key Configuration Options

- `setSsl(true)`: Enables SSL/TLS
- `setUseAlpn(true)`: Enables ALPN for HTTP/2 (required for gRPC)
- `setTrustOptions()`: Configures certificate trust (CA certificates)
- `setKeyCertOptions()`: Configures client certificate and key for mTLS
- `setVerifyHost()`: Controls hostname verification
- `setTrustAll()`: Controls whether to trust all certificates

For more details on Vert.x SSL configuration, see the [Vert.x documentation](https://vertx.io/docs/vertx-core/java/#ssl).
