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

import java.util.concurrent.CompletableFuture;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.grpc.GrpcService;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.lock.UnlockResponse;
import io.etcd.jetcd.support.Errors;
import io.etcd.jetcd.support.Util;

import static java.util.Objects.requireNonNull;

final class LockClient extends AbstractClient implements Lock {
    private final io.etcd.jetcd.api.lock.LockGrpcClient client;
    private final ByteSequence namespace;

    LockClient(GrpcService grpcService) {
        super(grpcService);

        this.client = io.etcd.jetcd.api.lock.LockGrpcClient.create(
            grpcService.getAuthenticatedGrpcClient(),
            grpcService.getServiceResolver().getTarget());
        this.namespace = grpcService.getNamespace();
    }

    @Override
    public CompletableFuture<LockResponse> lock(ByteSequence name, long leaseId) {
        requireNonNull(name);

        io.etcd.jetcd.api.lock.LockRequest request = io.etcd.jetcd.api.lock.LockRequest.newBuilder()
            .setName(Util.prefixNamespace(name, namespace))
            .setLease(leaseId)
            .build();

        return execute(
            () -> client.lock(request),
            responseFactory::newLockResponse,
            Errors::isRetryableForSafeRedoOp);
    }

    @Override
    public CompletableFuture<UnlockResponse> unlock(ByteSequence lockKey) {
        requireNonNull(lockKey);

        io.etcd.jetcd.api.lock.UnlockRequest request = io.etcd.jetcd.api.lock.UnlockRequest.newBuilder()
            .setKey(Util.prefixNamespace(lockKey, namespace))
            .build();

        return execute(
            () -> client.unlock(request),
            responseFactory::newUnlockResponse,
            Errors::isRetryableForSafeRedoOp);
    }
}
