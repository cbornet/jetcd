/*
 * Copyright 2016-2025 The jetcd authors
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

package io.etcd.jetcd.resolver;

import io.vertx.core.net.Address;
import io.vertx.core.net.AddressResolver;

/**
 * Base implementation for endpoint resolvers that holds common resolver and target fields.
 */
public abstract class AbstractEndpointResolver implements EndpointResolver {
    private final AddressResolver resolver;
    private final Address target;

    protected AbstractEndpointResolver(AddressResolver resolver, Address target) {
        this.resolver = resolver;
        this.target = target;
    }

    @Override
    public AddressResolver getResolver() {
        return resolver;
    }

    @Override
    public Address getTarget() {
        return target;
    }
}

