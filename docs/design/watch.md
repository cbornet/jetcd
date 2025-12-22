# Watch Implementation

## Overview

Each watcher manages its own dedicated gRPC stream with automatic reconnection.

## Architecture

```
WatchClient
  └── WatchConnection (per watcher)
        ├── WatchStateMachine    - Lifecycle management
        ├── WatchStream          - gRPC bidirectional stream
        ├── WatchResponseProcessor - Response parsing
        └── WatchReconnectionManager - Retry with backoff
```

## State Machine

States: `Connecting` → `Subscribing` → `Watching` ↔ `Reconnecting` → `Closed`

## Key Behaviors

1. **Watch creation** - Sends create request, waits for server confirmation
2. **Event processing** - Distributes events to listener, tracks revision
3. **Auto-resume** - On disconnect, reconnects with `revision = lastReceived + 1`
4. **Cancellation** - Sends cancel request, filters subsequent events
5. **Progress notify** - Propagates latest revision to all watchers

## Usage

```java
Watch.Watcher watcher = client.getWatchClient().watch(
    ByteSequence.from("/prefix"),
    WatchOption.builder().isPrefix(true).build(),
    new Watch.Listener() {
        @Override
        public void onNext(WatchResponse response) {
            response.getEvents().forEach(event -> {
                System.out.println(event.getKeyValue().getKey());
            });
        }

        @Override
        public void onError(Throwable throwable) {
            // Handle error
        }

        @Override
        public void onCompleted() {
            // Watch closed
        }
    });

// Later
watcher.close();
```

## Reconnection

- Exponential backoff on failures
- Resumes from last received revision
- Auth token refresh on token errors

