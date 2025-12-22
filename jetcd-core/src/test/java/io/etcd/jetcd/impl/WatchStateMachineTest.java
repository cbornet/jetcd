/*
 * Copyright 2016-2021 The jetcd authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.etcd.jetcd.impl;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for WatchStateMachine state transitions.
 */
@Tag("watch")
class WatchStateMachineTest {

    private Vertx vertx;
    private TestHandler handler;
    private WatchStateMachine stateMachine;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
        handler = new TestHandler();
        stateMachine = new WatchStateMachine(vertx, handler);
    }

    @AfterEach
    void tearDown() {
        if (vertx != null) {
            vertx.close();
        }
    }

    private void waitForEventLoop() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void testHappyPath() {
        // Initial state
        assertThat(stateMachine.currentState()).isInstanceOf(WatchStateMachine.State.Connecting.class);

        // Start
        stateMachine.start();
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onConnect");
        assertThat(stateMachine.currentState()).isInstanceOf(WatchStateMachine.State.Connecting.class);

        // Stream ready
        handler.clear();
        stateMachine.streamReady();
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onStateChange", "onSubscribe");
        assertThat(handler.lastOldState).isInstanceOf(WatchStateMachine.State.Connecting.class);
        assertThat(handler.lastNewState).isInstanceOf(WatchStateMachine.State.Subscribing.class);
        assertThat(stateMachine.currentState()).isInstanceOf(WatchStateMachine.State.Subscribing.class);

        // Watch created
        handler.clear();
        stateMachine.watchCreated();
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onStateChange", "onReady");
        assertThat(handler.lastOldState).isInstanceOf(WatchStateMachine.State.Subscribing.class);
        assertThat(handler.lastNewState).isInstanceOf(WatchStateMachine.State.Watching.class);
        assertThat(stateMachine.currentState()).isInstanceOf(WatchStateMachine.State.Watching.class);
        assertThat(stateMachine.isWatching()).isTrue();
    }

    @Test
    void testReconnectionFlow() {
        // Get to WATCHING state
        stateMachine.start();
        stateMachine.streamReady();
        stateMachine.watchCreated();
        waitForEventLoop();
        handler.clear();

        // Stream ends
        stateMachine.streamEnded();
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onStateChange", "onReconnect");
        assertThat(stateMachine.currentState()).isInstanceOf(WatchStateMachine.State.Reconnecting.class);

        // Reconnect succeeds
        handler.clear();
        stateMachine.reconnectSucceeded();
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onStateChange", "onConnect");
        assertThat(stateMachine.currentState()).isInstanceOf(WatchStateMachine.State.Connecting.class);
    }

    @Test
    void testStreamErrorTriggersReconnect() {
        // Get to WATCHING state
        stateMachine.start();
        stateMachine.streamReady();
        stateMachine.watchCreated();
        waitForEventLoop();
        handler.clear();

        // Stream error
        RuntimeException error = new RuntimeException("connection lost");
        stateMachine.streamError(error);
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onError", "onStateChange", "onReconnect");
        assertThat(handler.lastError).isEqualTo(error);
        assertThat(stateMachine.currentState()).isInstanceOf(WatchStateMachine.State.Reconnecting.class);
    }

    @Test
    void testReconnectFailed() {
        // Get to RECONNECTING state
        stateMachine.start();
        stateMachine.streamReady();
        stateMachine.watchCreated();
        stateMachine.streamEnded();
        waitForEventLoop();
        handler.clear();

        // Reconnect fails
        RuntimeException error = new RuntimeException("max retries exceeded");
        stateMachine.reconnectFailed(error);
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onStateChange", "onError", "onClose");
        assertThat(handler.lastError).isEqualTo(error);
        assertThat(stateMachine.currentState()).isInstanceOf(WatchStateMachine.State.Closed.class);
        assertThat(stateMachine.isClosed()).isTrue();
    }

    @Test
    void testWatchCanceled() {
        // Get to WATCHING state
        stateMachine.start();
        stateMachine.streamReady();
        stateMachine.watchCreated();
        waitForEventLoop();
        handler.clear();

        // Watch canceled by server
        RuntimeException error = new RuntimeException("watch canceled");
        stateMachine.watchCanceled(error);
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onStateChange", "onError", "onClose");
        assertThat(handler.lastError).isEqualTo(error);
        assertThat(stateMachine.currentState()).isInstanceOf(WatchStateMachine.State.Closed.class);
    }

    @Test
    void testCloseFromConnecting() {
        stateMachine.start();
        waitForEventLoop();
        handler.clear();

        stateMachine.close();
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onStateChange", "onClose");
        assertThat(stateMachine.isClosed()).isTrue();
    }

    @Test
    void testCloseFromWatching() {
        stateMachine.start();
        stateMachine.streamReady();
        stateMachine.watchCreated();
        waitForEventLoop();
        handler.clear();

        stateMachine.close();
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onStateChange", "onClose");
        assertThat(stateMachine.isClosed()).isTrue();
    }

    @Test
    void testCloseFromReconnecting() {
        stateMachine.start();
        stateMachine.streamReady();
        stateMachine.watchCreated();
        stateMachine.streamEnded();
        waitForEventLoop();
        handler.clear();

        stateMachine.close();
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onStateChange", "onClose");
        assertThat(stateMachine.isClosed()).isTrue();
    }

    @Test
    void testDoubleCloseIsIgnored() {
        stateMachine.start();
        stateMachine.close();
        waitForEventLoop();
        handler.clear();

        stateMachine.close();
        waitForEventLoop();
        assertThat(handler.events).isEmpty();
    }

    @Test
    void testInvalidStateTransitionStreamReady() {
        stateMachine.start();
        stateMachine.streamReady();
        stateMachine.watchCreated();
        waitForEventLoop();
        handler.clear();

        // streamReady in WATCHING state is invalid
        stateMachine.streamReady();
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onError");
        assertThat(handler.lastError).isInstanceOf(IllegalStateException.class);
        assertThat(handler.lastError.getMessage()).contains("streamReady()").contains("WATCHING");
    }

    @Test
    void testInvalidStateTransitionWatchCreated() {
        stateMachine.start();
        waitForEventLoop();
        handler.clear();

        // watchCreated in CONNECTING state is invalid
        stateMachine.watchCreated();
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onError");
        assertThat(handler.lastError).isInstanceOf(IllegalStateException.class);
        assertThat(handler.lastError.getMessage()).contains("watchCreated()").contains("CONNECTING");
    }

    @Test
    void testInvalidStateTransitionReconnectSucceeded() {
        stateMachine.start();
        stateMachine.streamReady();
        stateMachine.watchCreated();
        waitForEventLoop();
        handler.clear();

        // reconnectSucceeded in WATCHING state is invalid
        stateMachine.reconnectSucceeded();
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onError");
        assertThat(handler.lastError).isInstanceOf(IllegalStateException.class);
        assertThat(handler.lastError.getMessage()).contains("reconnectSucceeded()").contains("WATCHING");
    }

    @Test
    void testEventsIgnoredWhenClosed() {
        stateMachine.start();
        stateMachine.close();
        waitForEventLoop();
        handler.clear();

        // All these should be ignored when closed
        stateMachine.streamReady();
        stateMachine.watchCreated();
        stateMachine.streamEnded();
        stateMachine.streamError(new RuntimeException());
        stateMachine.reconnectSucceeded();
        stateMachine.reconnectFailed(new RuntimeException());
        waitForEventLoop();

        assertThat(handler.events).isEmpty();
    }

    @Test
    void testStreamEndedFromConnecting() {
        stateMachine.start();
        waitForEventLoop();
        handler.clear();

        // Stream can end before fully connected
        stateMachine.streamEnded();
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onStateChange", "onReconnect");
        assertThat(stateMachine.currentState()).isInstanceOf(WatchStateMachine.State.Reconnecting.class);
    }

    @Test
    void testStreamEndedFromSubscribing() {
        stateMachine.start();
        stateMachine.streamReady();
        waitForEventLoop();
        handler.clear();

        stateMachine.streamEnded();
        waitForEventLoop();
        assertThat(handler.events).containsExactly("onStateChange", "onReconnect");
        assertThat(stateMachine.currentState()).isInstanceOf(WatchStateMachine.State.Reconnecting.class);
    }

    /**
     * Test handler that records events for verification.
     */
    private static class TestHandler implements WatchStateMachine.Handler {
        final List<String> events = new ArrayList<>();
        volatile Throwable lastError;
        volatile WatchStateMachine.State lastOldState;
        volatile WatchStateMachine.State lastNewState;

        void clear() {
            events.clear();
            lastError = null;
            lastOldState = null;
            lastNewState = null;
        }

        @Override
        public void onConnect() {
            events.add("onConnect");
        }

        @Override
        public void onSubscribe() {
            events.add("onSubscribe");
        }

        @Override
        public void onReady() {
            events.add("onReady");
        }

        @Override
        public void onReconnect() {
            events.add("onReconnect");
        }

        @Override
        public void onClose() {
            events.add("onClose");
        }

        @Override
        public void onError(Throwable error) {
            events.add("onError");
            lastError = error;
        }

        @Override
        public void onStateChange(WatchStateMachine.State oldState, WatchStateMachine.State newState) {
            events.add("onStateChange");
            lastOldState = oldState;
            lastNewState = newState;
        }
    }
}
