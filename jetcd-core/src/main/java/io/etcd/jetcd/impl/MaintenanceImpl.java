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

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import io.etcd.jetcd.Maintenance;
import io.etcd.jetcd.api.*;
import io.etcd.jetcd.api.AlarmType;
import io.etcd.jetcd.maintenance.AlarmResponse;
import io.etcd.jetcd.maintenance.DefragmentResponse;
import io.etcd.jetcd.maintenance.HashKVResponse;
import io.etcd.jetcd.maintenance.MoveLeaderResponse;
import io.etcd.jetcd.maintenance.StatusResponse;

import static io.etcd.jetcd.common.Preconditions.checkArgument;
import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.toEtcdException;

/**
 * Implementation of maintenance client.
 */
final class MaintenanceImpl extends AbstractService implements Maintenance {
    private final MaintenanceGrpcClient client;

    MaintenanceImpl(GrpcService grpcService) {
        super(grpcService);

        io.etcd.jetcd.resolver.ServiceResolver serviceResolver = grpcService.getServiceResolver();
        client = MaintenanceGrpcClient.create(
            grpcService.getAuthenticatedGrpcClient(),
            (io.vertx.core.net.SocketAddress) serviceResolver.getTarget());
    }

    @Override
    public CompletableFuture<AlarmResponse> listAlarms() {
        AlarmRequest alarmRequest = AlarmRequest.newBuilder()
            .setAlarm(AlarmType.NONE)
            .setAction(AlarmRequest.AlarmAction.GET)
            .setMemberID(0)
            .build();

        return completable(client.alarm(alarmRequest), AlarmResponse::new);
    }

    @Override
    public CompletableFuture<AlarmResponse> alarmDisarm(io.etcd.jetcd.maintenance.AlarmMember member) {
        checkArgument(member.getMemberId() != 0, "the member id can not be 0");
        checkArgument(member.getAlarmType() != io.etcd.jetcd.maintenance.AlarmType.NONE, "alarm type can not be NONE");

        AlarmRequest alarmRequest = AlarmRequest.newBuilder()
            .setAlarm(AlarmType.NOSPACE)
            .setAction(AlarmRequest.AlarmAction.DEACTIVATE)
            .setMemberID(member.getMemberId())
            .build();

        return completable(client.alarm(alarmRequest), AlarmResponse::new);
    }

    @Override
    public CompletableFuture<DefragmentResponse> defragmentMember(String target) {
        // TODO: Implement target-specific client creation for defragmentMember
        // For now, use the default client
        return completable(
            client.defragment(DefragmentRequest.getDefaultInstance()),
            DefragmentResponse::new);
    }

    @Override
    public CompletableFuture<StatusResponse> statusMember(String target) {
        // TODO: Implement target-specific client creation for statusMember
        // For now, use the default client
        return completable(
            client.status(StatusRequest.getDefaultInstance()),
            StatusResponse::new);
    }

    @Override
    public CompletableFuture<MoveLeaderResponse> moveLeader(long transfereeID) {
        return completable(
            client.moveLeader(MoveLeaderRequest.newBuilder().setTargetID(transfereeID).build()),
            MoveLeaderResponse::new);
    }

    @Override
    public CompletableFuture<HashKVResponse> hashKV(String target, long rev) {
        // TODO: Implement target-specific client creation for hashKV
        // For now, use the default client
        return completable(
            client.hashKV(HashKVRequest.newBuilder().setRevision(rev).build()),
            HashKVResponse::new);
    }

    @Override
    public CompletableFuture<Long> snapshot(OutputStream outputStream) {
        final CompletableFuture<Long> answer = new CompletableFuture<>();
        final AtomicLong bytes = new AtomicLong(0);

        client.snapshot(SnapshotRequest.getDefaultInstance()).onComplete(ar -> {
            if (ar.failed()) {
                answer.completeExceptionally(toEtcdException(ar.cause()));
            } else {
                ar.result().handler(r -> {
                    try {
                        r.getBlob().writeTo(outputStream);
                        bytes.addAndGet(r.getBlob().size());
                    } catch (IOException e) {
                        answer.completeExceptionally(toEtcdException(e));
                    }
                });
                ar.result().endHandler(event -> {
                    answer.complete(bytes.get());
                });
                ar.result().exceptionHandler(e -> {
                    answer.completeExceptionally(toEtcdException(e));
                });
            }
        });

        return answer;
    }

    @Override
    public void snapshot(Maintenance.Listener listener) {
        client.snapshot(SnapshotRequest.getDefaultInstance()).onComplete(ar -> {
            if (ar.failed()) {
                listener.onError(toEtcdException(ar.cause()));
            } else {
                ar.result().handler(r -> listener.onNext(new io.etcd.jetcd.maintenance.SnapshotResponse(r)));
                ar.result().endHandler(event -> listener.onCompleted());
                ar.result().exceptionHandler(e -> listener.onError(toEtcdException(e)));
            }
        });
    }
}
