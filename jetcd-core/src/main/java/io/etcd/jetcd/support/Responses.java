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

package io.etcd.jetcd.support;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.api.DeleteRangeResponse;
import io.etcd.jetcd.api.RangeResponse;
import io.etcd.jetcd.auth.AuthDisableResponse;
import io.etcd.jetcd.auth.AuthEnableResponse;
import io.etcd.jetcd.auth.AuthRoleAddResponse;
import io.etcd.jetcd.auth.AuthRoleDeleteResponse;
import io.etcd.jetcd.auth.AuthRoleGetResponse;
import io.etcd.jetcd.auth.AuthRoleGrantPermissionResponse;
import io.etcd.jetcd.auth.AuthRoleListResponse;
import io.etcd.jetcd.auth.AuthRoleRevokePermissionResponse;
import io.etcd.jetcd.auth.AuthUserAddResponse;
import io.etcd.jetcd.auth.AuthUserChangePasswordResponse;
import io.etcd.jetcd.auth.AuthUserDeleteResponse;
import io.etcd.jetcd.auth.AuthUserGetResponse;
import io.etcd.jetcd.auth.AuthUserGrantRoleResponse;
import io.etcd.jetcd.auth.AuthUserListResponse;
import io.etcd.jetcd.auth.AuthUserRevokeRoleResponse;
import io.etcd.jetcd.cluster.MemberAddResponse;
import io.etcd.jetcd.cluster.MemberListResponse;
import io.etcd.jetcd.cluster.MemberPromoteResponse;
import io.etcd.jetcd.cluster.MemberRemoveResponse;
import io.etcd.jetcd.cluster.MemberUpdateResponse;
import io.etcd.jetcd.election.CampaignResponse;
import io.etcd.jetcd.election.LeaderResponse;
import io.etcd.jetcd.election.ProclaimResponse;
import io.etcd.jetcd.election.ResignResponse;
import io.etcd.jetcd.kv.CompactResponse;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseRevokeResponse;
import io.etcd.jetcd.lease.LeaseTimeToLiveResponse;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.lock.UnlockResponse;
import io.etcd.jetcd.maintenance.AlarmResponse;
import io.etcd.jetcd.maintenance.DefragmentResponse;
import io.etcd.jetcd.maintenance.HashKVResponse;
import io.etcd.jetcd.maintenance.MoveLeaderResponse;
import io.etcd.jetcd.maintenance.SnapshotResponse;
import io.etcd.jetcd.maintenance.StatusResponse;
import io.etcd.jetcd.watch.WatchResponse;

/**
 * Factory for creating Response objects.
 * Provides static factory methods for direct response creation and a namespaced factory for method references.
 */
public final class Responses {

    private Responses() {
    }

    // ============================================
    // Static factory methods (with namespace)
    // ============================================

    // KV responses
    public static PutResponse newPutResponse(io.etcd.jetcd.api.PutResponse response, ByteSequence namespace) {
        return new PutResponse(response, namespace);
    }

    public static GetResponse newGetResponse(RangeResponse response, ByteSequence namespace) {
        return new GetResponse(response, namespace);
    }

    public static DeleteResponse newDeleteResponse(DeleteRangeResponse response, ByteSequence namespace) {
        return new DeleteResponse(response, namespace);
    }

    public static TxnResponse newTxnResponse(io.etcd.jetcd.api.TxnResponse response, ByteSequence namespace) {
        return new TxnResponse(response, namespace);
    }

    // Watch responses
    public static WatchResponse newWatchResponse(io.etcd.jetcd.api.WatchResponse response, ByteSequence namespace) {
        return new WatchResponse(response, namespace);
    }

    // Election responses
    public static LeaderResponse newLeaderResponse(io.etcd.jetcd.api.LeaderResponse response, ByteSequence namespace) {
        return new LeaderResponse(response, namespace);
    }

    // Lock responses
    public static LockResponse newLockResponse(io.etcd.jetcd.api.lock.LockResponse response, ByteSequence namespace) {
        return new LockResponse(response, namespace);
    }

    // ============================================
    // Static factory methods (without namespace)
    // ============================================

    // KV responses
    public static CompactResponse newCompactResponse(io.etcd.jetcd.api.CompactionResponse response) {
        return new CompactResponse(response);
    }

    // Lease responses
    public static LeaseGrantResponse newLeaseGrantResponse(io.etcd.jetcd.api.LeaseGrantResponse response) {
        return new LeaseGrantResponse(response);
    }

    public static LeaseRevokeResponse newLeaseRevokeResponse(io.etcd.jetcd.api.LeaseRevokeResponse response) {
        return new LeaseRevokeResponse(response);
    }

    public static LeaseTimeToLiveResponse newLeaseTimeToLiveResponse(
        io.etcd.jetcd.api.LeaseTimeToLiveResponse response) {
        return new LeaseTimeToLiveResponse(response);
    }

    // Election responses
    public static CampaignResponse newCampaignResponse(io.etcd.jetcd.api.CampaignResponse response) {
        return new CampaignResponse(response);
    }

    public static ProclaimResponse newProclaimResponse(io.etcd.jetcd.api.ProclaimResponse response) {
        return new ProclaimResponse(response);
    }

    public static ResignResponse newResignResponse(io.etcd.jetcd.api.ResignResponse response) {
        return new ResignResponse(response);
    }

    // Lock responses
    public static UnlockResponse newUnlockResponse(io.etcd.jetcd.api.lock.UnlockResponse response) {
        return new UnlockResponse(response);
    }

    // Maintenance responses
    public static AlarmResponse newAlarmResponse(io.etcd.jetcd.api.AlarmResponse response) {
        return new AlarmResponse(response);
    }

    public static DefragmentResponse newDefragmentResponse(io.etcd.jetcd.api.DefragmentResponse response) {
        return new DefragmentResponse(response);
    }

    public static HashKVResponse newHashKVResponse(io.etcd.jetcd.api.HashKVResponse response) {
        return new HashKVResponse(response);
    }

    public static MoveLeaderResponse newMoveLeaderResponse(io.etcd.jetcd.api.MoveLeaderResponse response) {
        return new MoveLeaderResponse(response);
    }

    public static SnapshotResponse newSnapshotResponse(io.etcd.jetcd.api.SnapshotResponse response) {
        return new SnapshotResponse(response);
    }

    public static StatusResponse newStatusResponse(io.etcd.jetcd.api.StatusResponse response) {
        return new StatusResponse(response);
    }

    // Cluster responses
    public static MemberAddResponse newMemberAddResponse(io.etcd.jetcd.api.MemberAddResponse response) {
        return new MemberAddResponse(response);
    }

    public static MemberListResponse newMemberListResponse(io.etcd.jetcd.api.MemberListResponse response) {
        return new MemberListResponse(response);
    }

    public static MemberPromoteResponse newMemberPromoteResponse(io.etcd.jetcd.api.MemberPromoteResponse response) {
        return new MemberPromoteResponse(response);
    }

    public static MemberRemoveResponse newMemberRemoveResponse(io.etcd.jetcd.api.MemberRemoveResponse response) {
        return new MemberRemoveResponse(response);
    }

    public static MemberUpdateResponse newMemberUpdateResponse(io.etcd.jetcd.api.MemberUpdateResponse response) {
        return new MemberUpdateResponse(response);
    }

    // Auth responses
    public static AuthDisableResponse newAuthDisableResponse(io.etcd.jetcd.api.AuthDisableResponse response) {
        return new AuthDisableResponse(response);
    }

    public static AuthEnableResponse newAuthEnableResponse(io.etcd.jetcd.api.AuthEnableResponse response) {
        return new AuthEnableResponse(response);
    }

    public static AuthRoleAddResponse newAuthRoleAddResponse(io.etcd.jetcd.api.AuthRoleAddResponse response) {
        return new AuthRoleAddResponse(response);
    }

    public static AuthRoleDeleteResponse newAuthRoleDeleteResponse(io.etcd.jetcd.api.AuthRoleDeleteResponse response) {
        return new AuthRoleDeleteResponse(response);
    }

    public static AuthRoleGetResponse newAuthRoleGetResponse(io.etcd.jetcd.api.AuthRoleGetResponse response) {
        return new AuthRoleGetResponse(response);
    }

    public static AuthRoleGrantPermissionResponse newAuthRoleGrantPermissionResponse(
        io.etcd.jetcd.api.AuthRoleGrantPermissionResponse response) {
        return new AuthRoleGrantPermissionResponse(response);
    }

    public static AuthRoleListResponse newAuthRoleListResponse(io.etcd.jetcd.api.AuthRoleListResponse response) {
        return new AuthRoleListResponse(response);
    }

    public static AuthRoleRevokePermissionResponse newAuthRoleRevokePermissionResponse(
        io.etcd.jetcd.api.AuthRoleRevokePermissionResponse response) {
        return new AuthRoleRevokePermissionResponse(response);
    }

    public static AuthUserAddResponse newAuthUserAddResponse(io.etcd.jetcd.api.AuthUserAddResponse response) {
        return new AuthUserAddResponse(response);
    }

    public static AuthUserChangePasswordResponse newAuthUserChangePasswordResponse(
        io.etcd.jetcd.api.AuthUserChangePasswordResponse response) {
        return new AuthUserChangePasswordResponse(response);
    }

    public static AuthUserDeleteResponse newAuthUserDeleteResponse(io.etcd.jetcd.api.AuthUserDeleteResponse response) {
        return new AuthUserDeleteResponse(response);
    }

    public static AuthUserGetResponse newAuthUserGetResponse(io.etcd.jetcd.api.AuthUserGetResponse response) {
        return new AuthUserGetResponse(response);
    }

    public static AuthUserGrantRoleResponse newAuthUserGrantRoleResponse(
        io.etcd.jetcd.api.AuthUserGrantRoleResponse response) {
        return new AuthUserGrantRoleResponse(response);
    }

    public static AuthUserListResponse newAuthUserListResponse(io.etcd.jetcd.api.AuthUserListResponse response) {
        return new AuthUserListResponse(response);
    }

    public static AuthUserRevokeRoleResponse newAuthUserRevokeRoleResponse(
        io.etcd.jetcd.api.AuthUserRevokeRoleResponse response) {
        return new AuthUserRevokeRoleResponse(response);
    }

    // ============================================
    // Namespaced factory for method references
    // ============================================

    /**
     * Returns a namespaced factory for creating namespace-aware responses.
     * Use this to cache the factory in client code for cleaner method references.
     *
     * @param  namespace the namespace for response construction
     * @return           a namespaced factory instance
     */
    public static Namespaced namespaced(ByteSequence namespace) {
        return new Namespaced(namespace);
    }

    /**
     * Factory for creating namespace-aware Response objects.
     * Designed to be cached in client implementations for method reference usage.
     *
     * @param namespace the namespace to use for responses
     */
    public record Namespaced(ByteSequence namespace) {

        // Namespaced responses
        public PutResponse newPutResponse(io.etcd.jetcd.api.PutResponse response) {
            return Responses.newPutResponse(response, namespace);
        }

        public GetResponse newGetResponse(RangeResponse response) {
            return Responses.newGetResponse(response, namespace);
        }

        public DeleteResponse newDeleteResponse(DeleteRangeResponse response) {
            return Responses.newDeleteResponse(response, namespace);
        }

        public TxnResponse newTxnResponse(io.etcd.jetcd.api.TxnResponse response) {
            return Responses.newTxnResponse(response, namespace);
        }

        public WatchResponse newWatchResponse(io.etcd.jetcd.api.WatchResponse response) {
            return Responses.newWatchResponse(response, namespace);
        }

        public LeaderResponse newLeaderResponse(io.etcd.jetcd.api.LeaderResponse response) {
            return Responses.newLeaderResponse(response, namespace);
        }

        public LockResponse newLockResponse(io.etcd.jetcd.api.lock.LockResponse response) {
            return Responses.newLockResponse(response, namespace);
        }

        // Non-namespaced responses (delegate to static methods)

        // KV responses
        public CompactResponse newCompactResponse(io.etcd.jetcd.api.CompactionResponse response) {
            return Responses.newCompactResponse(response);
        }

        // Lease responses
        public LeaseGrantResponse newLeaseGrantResponse(io.etcd.jetcd.api.LeaseGrantResponse response) {
            return Responses.newLeaseGrantResponse(response);
        }

        public LeaseRevokeResponse newLeaseRevokeResponse(io.etcd.jetcd.api.LeaseRevokeResponse response) {
            return Responses.newLeaseRevokeResponse(response);
        }

        public LeaseTimeToLiveResponse newLeaseTimeToLiveResponse(io.etcd.jetcd.api.LeaseTimeToLiveResponse response) {
            return Responses.newLeaseTimeToLiveResponse(response);
        }

        // Election responses
        public CampaignResponse newCampaignResponse(io.etcd.jetcd.api.CampaignResponse response) {
            return Responses.newCampaignResponse(response);
        }

        public ProclaimResponse newProclaimResponse(io.etcd.jetcd.api.ProclaimResponse response) {
            return Responses.newProclaimResponse(response);
        }

        public ResignResponse newResignResponse(io.etcd.jetcd.api.ResignResponse response) {
            return Responses.newResignResponse(response);
        }

        // Lock responses
        public UnlockResponse newUnlockResponse(io.etcd.jetcd.api.lock.UnlockResponse response) {
            return Responses.newUnlockResponse(response);
        }

        // Maintenance responses
        public AlarmResponse newAlarmResponse(io.etcd.jetcd.api.AlarmResponse response) {
            return Responses.newAlarmResponse(response);
        }

        public DefragmentResponse newDefragmentResponse(io.etcd.jetcd.api.DefragmentResponse response) {
            return Responses.newDefragmentResponse(response);
        }

        public HashKVResponse newHashKVResponse(io.etcd.jetcd.api.HashKVResponse response) {
            return Responses.newHashKVResponse(response);
        }

        public MoveLeaderResponse newMoveLeaderResponse(io.etcd.jetcd.api.MoveLeaderResponse response) {
            return Responses.newMoveLeaderResponse(response);
        }

        public SnapshotResponse newSnapshotResponse(io.etcd.jetcd.api.SnapshotResponse response) {
            return Responses.newSnapshotResponse(response);
        }

        public StatusResponse newStatusResponse(io.etcd.jetcd.api.StatusResponse response) {
            return Responses.newStatusResponse(response);
        }

        // Cluster responses
        public MemberAddResponse newMemberAddResponse(io.etcd.jetcd.api.MemberAddResponse response) {
            return Responses.newMemberAddResponse(response);
        }

        public MemberListResponse newMemberListResponse(io.etcd.jetcd.api.MemberListResponse response) {
            return Responses.newMemberListResponse(response);
        }

        public MemberPromoteResponse newMemberPromoteResponse(io.etcd.jetcd.api.MemberPromoteResponse response) {
            return Responses.newMemberPromoteResponse(response);
        }

        public MemberRemoveResponse newMemberRemoveResponse(io.etcd.jetcd.api.MemberRemoveResponse response) {
            return Responses.newMemberRemoveResponse(response);
        }

        public MemberUpdateResponse newMemberUpdateResponse(io.etcd.jetcd.api.MemberUpdateResponse response) {
            return Responses.newMemberUpdateResponse(response);
        }

        // Auth responses
        public AuthDisableResponse newAuthDisableResponse(io.etcd.jetcd.api.AuthDisableResponse response) {
            return Responses.newAuthDisableResponse(response);
        }

        public AuthEnableResponse newAuthEnableResponse(io.etcd.jetcd.api.AuthEnableResponse response) {
            return Responses.newAuthEnableResponse(response);
        }

        public AuthRoleAddResponse newAuthRoleAddResponse(io.etcd.jetcd.api.AuthRoleAddResponse response) {
            return Responses.newAuthRoleAddResponse(response);
        }

        public AuthRoleDeleteResponse newAuthRoleDeleteResponse(io.etcd.jetcd.api.AuthRoleDeleteResponse response) {
            return Responses.newAuthRoleDeleteResponse(response);
        }

        public AuthRoleGetResponse newAuthRoleGetResponse(io.etcd.jetcd.api.AuthRoleGetResponse response) {
            return Responses.newAuthRoleGetResponse(response);
        }

        public AuthRoleGrantPermissionResponse newAuthRoleGrantPermissionResponse(
            io.etcd.jetcd.api.AuthRoleGrantPermissionResponse response) {
            return Responses.newAuthRoleGrantPermissionResponse(response);
        }

        public AuthRoleListResponse newAuthRoleListResponse(io.etcd.jetcd.api.AuthRoleListResponse response) {
            return Responses.newAuthRoleListResponse(response);
        }

        public AuthRoleRevokePermissionResponse newAuthRoleRevokePermissionResponse(
            io.etcd.jetcd.api.AuthRoleRevokePermissionResponse response) {
            return Responses.newAuthRoleRevokePermissionResponse(response);
        }

        public AuthUserAddResponse newAuthUserAddResponse(io.etcd.jetcd.api.AuthUserAddResponse response) {
            return Responses.newAuthUserAddResponse(response);
        }

        public AuthUserChangePasswordResponse newAuthUserChangePasswordResponse(
            io.etcd.jetcd.api.AuthUserChangePasswordResponse response) {
            return Responses.newAuthUserChangePasswordResponse(response);
        }

        public AuthUserDeleteResponse newAuthUserDeleteResponse(io.etcd.jetcd.api.AuthUserDeleteResponse response) {
            return Responses.newAuthUserDeleteResponse(response);
        }

        public AuthUserGetResponse newAuthUserGetResponse(io.etcd.jetcd.api.AuthUserGetResponse response) {
            return Responses.newAuthUserGetResponse(response);
        }

        public AuthUserGrantRoleResponse newAuthUserGrantRoleResponse(
            io.etcd.jetcd.api.AuthUserGrantRoleResponse response) {
            return Responses.newAuthUserGrantRoleResponse(response);
        }

        public AuthUserListResponse newAuthUserListResponse(io.etcd.jetcd.api.AuthUserListResponse response) {
            return Responses.newAuthUserListResponse(response);
        }

        public AuthUserRevokeRoleResponse newAuthUserRevokeRoleResponse(
            io.etcd.jetcd.api.AuthUserRevokeRoleResponse response) {
            return Responses.newAuthUserRevokeRoleResponse(response);
        }
    }
}
