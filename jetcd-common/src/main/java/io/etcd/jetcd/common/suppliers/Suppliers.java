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

package io.etcd.jetcd.common.suppliers;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Utility class for creating specialized Supplier implementations.
 */
public final class Suppliers {

    private Suppliers() {
    }

    /**
     * Returns a supplier that caches the instance retrieved during the first call to {@code get()}
     * and returns that value on subsequent calls. The returned supplier is thread-safe.
     *
     * @param  delegate the underlying supplier
     * @param  <T>      the type of results supplied by this supplier
     * @return          a memoizing supplier
     */
    public static <T> Supplier<T> memoizing(Supplier<T> delegate) {
        return new MemorizingSupplier<>(delegate);
    }


    /**
     * Returns a supplier that caches the instance retrieved during the first call to {@code get()}
     * and returns that value on subsequent calls. The returned supplier is thread-safe and
     * implements {@link AutoCloseable} to properly clean up the cached resource.
     *
     * @param  delegate the underlying supplier
     * @param  <T>      the type of results supplied by this supplier
     * @return          a memoizing supplier that implements AutoCloseable
     */
    public static <T extends AutoCloseable> CloseableSupplier<T> memoizingCloseable(Supplier<T> delegate) {
        return new MemorizingCloseableSupplier<>(delegate);
    }

    /**
     * A supplier that memoizes the result of another supplier and implements AutoCloseable
     * to properly clean up the cached resource.
     */
    public static class MemorizingCloseableSupplier<T extends AutoCloseable> implements CloseableSupplier<T> {
        final Supplier<T> delegate;
        volatile boolean initialized;
        T value;

        MemorizingCloseableSupplier(Supplier<T> delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public T get() {
            // A 2-field variant of Double Checked Locking.
            if (!initialized) {
                synchronized (this) {
                    if (!initialized) {
                        T t = delegate.get();
                        value = t;
                        initialized = true;
                        return t;
                    }
                }
            }
            return value;
        }

        @Override
        public String toString() {
            return "Suppliers.memoizingCloseable("
                + (initialized ? "<supplier that returned " + value + ">" : delegate)
                + ")";
        }

        @Override
        public void close() throws Exception {
            if (initialized) {
                synchronized (this) {
                    if (initialized) {
                        if (value != null) {
                            value.close();
                            value = null;
                        }
                        initialized = false;
                    }
                }
            }
        }
    }

    /**
     * A supplier that memoizes the result of another supplier.
     */
    public static class MemorizingSupplier<T> implements Supplier<T>{
        final Supplier<T> delegate;
        volatile boolean initialized;
        T value;

        MemorizingSupplier(Supplier<T> delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public T get() {
            // A 2-field variant of Double Checked Locking.
            if (!initialized) {
                synchronized (this) {
                    if (!initialized) {
                        T t = delegate.get();
                        value = t;
                        initialized = true;
                        return t;
                    }
                }
            }
            return value;
        }

        @Override
        public String toString() {
            return "Suppliers.memoizing("
                + (initialized ? "<supplier that returned " + value + ">" : delegate)
                + ")";
        }
    }
}
