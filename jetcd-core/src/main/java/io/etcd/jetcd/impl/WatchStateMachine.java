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

import io.vertx.core.Vertx;

/**
 * State machine for watch lifecycle management.
 *
 * <p>
 * All state transitions happen on the Vert.x event loop, ensuring thread safety
 * without explicit synchronization.
 * </p>
 *
 * <p>
 * State flow:
 * </p>
 *
 * <pre>
 * CONNECTING → SUBSCRIBING → WATCHING ←→ RECONNECTING
 *     ↓            ↓            ↓              ↓
 *                      CLOSED (terminal)
 * </pre>
 */
final class WatchStateMachine {

    sealed interface State {
        /**
         * Initial state - connecting to gRPC stream.
         */
        record Connecting() implements State {
            @Override
            public String toString() {
                return "CONNECTING";
            }
        }

        /**
         * Stream connected - subscribe request sent, awaiting confirmation.
         */
        record Subscribing() implements State {
            @Override
            public String toString() {
                return "SUBSCRIBING";
            }
        }

        /**
         * Watch active - receiving events.
         */
        record Watching() implements State {
            @Override
            public String toString() {
                return "WATCHING";
            }
        }

        /**
         * Connection lost - attempting to reconnect.
         */
        record Reconnecting() implements State {
            @Override
            public String toString() {
                return "RECONNECTING";
            }
        }

        /**
         * Terminal state - watcher closed.
         */
        record Closed() implements State {
            @Override
            public String toString() {
                return "CLOSED";
            }
        }
    }

    /**
     * Handler for state transition callbacks.
     * All callbacks are invoked on the Vert.x event loop.
     */
    interface Handler {
        /**
         * Called when connection should be initiated.
         */
        void onConnect();

        /**
         * Called when stream is ready and watch should be subscribed.
         */
        void onSubscribe();

        /**
         * Called when watch is confirmed and ready to receive events.
         */
        void onReady();

        /**
         * Called when reconnection should be attempted.
         */
        void onReconnect();

        /**
         * Called when watcher should be cleaned up.
         */
        void onClose();

        /**
         * Called when an error should be reported to the listener.
         */
        void onError(Throwable error);

        /**
         * Called when the state changes.
         */
        void onStateChange(State oldState, State newState);
    }

    private final Vertx vertx;
    private final Handler handler;
    private volatile State state = new State.Connecting();

    WatchStateMachine(Vertx vertx, Handler handler) {
        this.vertx = vertx;
        this.handler = handler;
    }

    /**
     * Starts the state machine by initiating connection.
     */
    void start() {
        runOnEventLoop(() -> {
            if (state instanceof State.Connecting) {
                handler.onConnect();
            } else {
                handler.onError(new IllegalStateException("start() called in state: " + state));
            }
        });
    }

    /**
     * Called when the gRPC stream is ready for writing.
     */
    void streamReady() {
        runOnEventLoop(() -> {
            if (state instanceof State.Connecting) {
                State oldState = state;
                state = new State.Subscribing();
                handler.onStateChange(oldState, state);
                handler.onSubscribe();
            } else if (!(state instanceof State.Closed)) {
                handler.onError(new IllegalStateException("streamReady() called in state: " + state));
            }
        });
    }

    /**
     * Called when the watch is created/confirmed by the server.
     */
    void watchCreated() {
        runOnEventLoop(() -> {
            if (state instanceof State.Subscribing) {
                State oldState = state;
                state = new State.Watching();
                handler.onStateChange(oldState, state);
                handler.onReady();
            } else if (!(state instanceof State.Closed)) {
                handler.onError(new IllegalStateException("watchCreated() called in state: " + state));
            }
        });
    }

    /**
     * Called when the watch is canceled by the server.
     */
    void watchCanceled(Throwable error) {
        runOnEventLoop(() -> {
            if (!(state instanceof State.Closed)) {
                State oldState = state;
                state = new State.Closed();
                handler.onStateChange(oldState, state);
                handler.onError(error);
                handler.onClose();
            }
        });
    }

    /**
     * Called when the stream ends normally.
     */
    void streamEnded() {
        runOnEventLoop(() -> {
            if (state instanceof State.Watching || state instanceof State.Subscribing || state instanceof State.Connecting) {
                State oldState = state;
                state = new State.Reconnecting();
                handler.onStateChange(oldState, state);
                handler.onReconnect();
            } else if (!(state instanceof State.Closed) && !(state instanceof State.Reconnecting)) {
                handler.onError(new IllegalStateException("streamEnded() called in state: " + state));
            }
        });
    }

    /**
     * Called when the stream encounters an error.
     */
    void streamError(Throwable error) {
        runOnEventLoop(() -> {
            if (state instanceof State.Watching || state instanceof State.Subscribing || state instanceof State.Connecting) {
                State oldState = state;
                handler.onError(error);
                state = new State.Reconnecting();
                handler.onStateChange(oldState, state);
                handler.onReconnect();
            } else if (!(state instanceof State.Closed) && !(state instanceof State.Reconnecting)) {
                handler.onError(new IllegalStateException("streamError() called in state: " + state));
            }
        });
    }

    /**
     * Called when reconnection succeeds - restarts the connection flow.
     */
    void reconnectSucceeded() {
        runOnEventLoop(() -> {
            if (state instanceof State.Reconnecting) {
                State oldState = state;
                state = new State.Connecting();
                handler.onStateChange(oldState, state);
                handler.onConnect();
            } else if (!(state instanceof State.Closed)) {
                handler.onError(new IllegalStateException("reconnectSucceeded() called in state: " + state));
            }
        });
    }

    /**
     * Called when reconnection fails after max retries.
     */
    void reconnectFailed(Throwable error) {
        runOnEventLoop(() -> {
            if (state instanceof State.Reconnecting) {
                State oldState = state;
                state = new State.Closed();
                handler.onStateChange(oldState, state);
                handler.onError(error);
                handler.onClose();
            } else if (!(state instanceof State.Closed)) {
                handler.onError(new IllegalStateException("reconnectFailed() called in state: " + state));
            }
        });
    }

    /**
     * Called to close the watcher.
     */
    void close() {
        runOnEventLoop(() -> {
            if (!(state instanceof State.Closed)) {
                State oldState = state;
                state = new State.Closed();
                handler.onStateChange(oldState, state);
                handler.onClose();
            }
        });
    }

    /**
     * Returns the current state.
     */
    State currentState() {
        return state;
    }

    /**
     * Returns true if the watcher is closed.
     */
    boolean isClosed() {
        return state instanceof State.Closed;
    }

    /**
     * Returns true if the watcher is actively watching.
     */
    boolean isWatching() {
        return state instanceof State.Watching;
    }

    private void runOnEventLoop(Runnable action) {
        vertx.runOnContext(v -> action.run());
    }
}
