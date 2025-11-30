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
import io.etcd.jetcd.Election;
import io.etcd.jetcd.election.CampaignResponse;
import io.etcd.jetcd.election.LeaderKey;
import io.etcd.jetcd.election.LeaderResponse;
import io.etcd.jetcd.election.NoLeaderException;
import io.etcd.jetcd.election.NotLeaderException;
import io.etcd.jetcd.election.ProclaimResponse;
import io.etcd.jetcd.election.ResignResponse;
import io.etcd.jetcd.support.Errors;
import io.etcd.jetcd.support.Util;
import io.vertx.grpc.client.InvalidStatusException;

import com.google.protobuf.ByteString;

import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.toEtcdException;
import static java.util.Objects.requireNonNull;

final class ElectionService extends AbstractService implements Election {
    private final io.etcd.jetcd.api.ElectionGrpcClient client;
    private final ByteSequence namespace;

    ElectionService(GrpcService grpcService) {
        super(grpcService);

        io.etcd.jetcd.resolver.ServiceResolver<?> serviceResolver = grpcService.getServiceResolver();
        this.client = io.etcd.jetcd.api.ElectionGrpcClient.create(
            grpcService.getAuthenticatedGrpcClient(),
            serviceResolver.getTarget(io.vertx.core.net.SocketAddress.class));
        this.namespace = grpcService.getNamespace();
    }

    @Override
    public CompletableFuture<CampaignResponse> campaign(ByteSequence electionName, long leaseId, ByteSequence proposal) {
        requireNonNull(electionName, "election name should not be null");
        requireNonNull(proposal, "proposal should not be null");

        io.etcd.jetcd.api.CampaignRequest request = io.etcd.jetcd.api.CampaignRequest.newBuilder()
            .setName(Util.prefixNamespace(electionName, namespace))
            .setValue(ByteString.copyFrom(proposal.getBytes()))
            .setLease(leaseId)
            .build();

        return wrapConvertException(
            execute(
                () -> client.campaign(request),
                CampaignResponse::new,
                Errors::isRetryableForNoSafeRedoOp),
            false);
    }

    @Override
    public CompletableFuture<ProclaimResponse> proclaim(LeaderKey leaderKey, ByteSequence proposal) {
        requireNonNull(leaderKey, "leader key should not be null");
        requireNonNull(proposal, "proposal should not be null");

        io.etcd.jetcd.api.ProclaimRequest request = io.etcd.jetcd.api.ProclaimRequest.newBuilder()
            .setLeader(
                io.etcd.jetcd.api.LeaderKey.newBuilder()
                    .setKey(ByteString.copyFrom(leaderKey.getKey().getBytes()))
                    .setName(ByteString.copyFrom(leaderKey.getName().getBytes()))
                    .setLease(leaderKey.getLease())
                    .setRev(leaderKey.getRevision())
                    .build())
            .setValue(ByteString.copyFrom(proposal.getBytes()))
            .build();

        return wrapConvertException(
            execute(
                () -> client.proclaim(request),
                ProclaimResponse::new,
                Errors::isRetryableForNoSafeRedoOp),
            false);
    }

    @Override
    public CompletableFuture<LeaderResponse> leader(ByteSequence electionName) {
        requireNonNull(electionName, "election name should not be null");

        io.etcd.jetcd.api.LeaderRequest request = io.etcd.jetcd.api.LeaderRequest.newBuilder()
            .setName(Util.prefixNamespace(electionName, namespace))
            .build();

        return wrapConvertException(
            execute(
                () -> client.leader(request),
                response -> new LeaderResponse(response, namespace),
                Errors::isRetryableForNoSafeRedoOp),
            true);
    }

    @Override
    public void observe(ByteSequence electionName, Listener listener) {
        requireNonNull(electionName, "election name should not be null");
        requireNonNull(listener, "listener should not be null");

        io.etcd.jetcd.api.LeaderRequest request = io.etcd.jetcd.api.LeaderRequest.newBuilder()
            .setName(Util.prefixNamespace(electionName, namespace))
            .build();

        client.observe(request).onComplete(ar -> {
            if (ar.failed()) {
                listener.onError(toEtcdException(ar.cause()));
            } else {
                ar.result().handler(value -> listener.onNext(new LeaderResponse(value, namespace)));
                ar.result().endHandler(ignored -> listener.onCompleted());
                ar.result().exceptionHandler(error -> listener.onError(toEtcdException(error)));
            }
        });
    }

    @Override
    public CompletableFuture<ResignResponse> resign(LeaderKey leaderKey) {
        requireNonNull(leaderKey, "leader key should not be null");

        io.etcd.jetcd.api.ResignRequest request = io.etcd.jetcd.api.ResignRequest.newBuilder()
            .setLeader(
                io.etcd.jetcd.api.LeaderKey.newBuilder()
                    .setKey(ByteString.copyFrom(leaderKey.getKey().getBytes()))
                    .setName(ByteString.copyFrom(leaderKey.getName().getBytes()))
                    .setLease(leaderKey.getLease())
                    .setRev(leaderKey.getRevision())
                    .build())
            .build();

        return wrapConvertException(
            execute(
                () -> client.resign(request),
                ResignResponse::new,
                Errors::isRetryableForNoSafeRedoOp),
            false);
    }

    private <S> CompletableFuture<S> wrapConvertException(CompletableFuture<S> future, boolean isLeaderQuery) {
        return future.exceptionally(e -> {
            throw convertException(e, isLeaderQuery);
        });
    }

    private RuntimeException convertException(Throwable e, boolean isLeaderQuery) {
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof InvalidStatusException invalidStatusException) {
                if (invalidStatusException.actualStatus() == io.vertx.grpc.common.GrpcStatus.UNKNOWN && isLeaderQuery) {
                    return new NoLeaderException();
                } else if (invalidStatusException.actualStatus() == io.vertx.grpc.common.GrpcStatus.UNKNOWN) {
                    return new NotLeaderException();
                }
            }
            cause = cause.getCause();
        }
        return toEtcdException(e);
    }
}

