/*
 * Copyright 2017 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.etcd.jetcd.common.exception;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import io.vertx.grpc.common.GrpcStatus;

/**
 * ErrorCode is a wrapper around gRPC Error code.
 *
 * <P>
 * Modification Notice:
 * This is a modification of ErrorCode.java from Google-cloud-spanner java api.
 * Updated to use Vert.x GrpcStatus instead of grpc-java Status.
 */
public enum ErrorCode {

    CANCELLED(GrpcStatus.CANCELLED),
    UNKNOWN(GrpcStatus.UNKNOWN),
    INVALID_ARGUMENT(GrpcStatus.INVALID_ARGUMENT),
    DEADLINE_EXCEEDED(GrpcStatus.DEADLINE_EXCEEDED),
    NOT_FOUND(GrpcStatus.NOT_FOUND),
    ALREADY_EXISTS(GrpcStatus.ALREADY_EXISTS),
    PERMISSION_DENIED(GrpcStatus.PERMISSION_DENIED),
    UNAUTHENTICATED(GrpcStatus.UNAUTHENTICATED),
    RESOURCE_EXHAUSTED(GrpcStatus.RESOURCE_EXHAUSTED),
    FAILED_PRECONDITION(GrpcStatus.FAILED_PRECONDITION),
    ABORTED(GrpcStatus.ABORTED),
    OUT_OF_RANGE(GrpcStatus.OUT_OF_RANGE),
    UNIMPLEMENTED(GrpcStatus.UNIMPLEMENTED),
    INTERNAL(GrpcStatus.INTERNAL),
    UNAVAILABLE(GrpcStatus.UNAVAILABLE),
    DATA_LOSS(GrpcStatus.DATA_LOSS),;

    private static final Map<Integer, ErrorCode> errorByRpcCode;

    static {
        Map<Integer, ErrorCode> realMap = new LinkedHashMap<>();
        for (ErrorCode errorCode : ErrorCode.values()) {
            realMap.put(errorCode.getCode(), errorCode);
        }
        errorByRpcCode = Collections.unmodifiableMap(realMap);
    }

    private final int code;

    ErrorCode(GrpcStatus status) {
        this.code = status.code;
    }

    int getCode() {
        return this.code;
    }

    /**
     * Returns the error code represents by {@code name}, or {@code defaultValue} if {@code name} does
     * not map to a known code.
     */
    static ErrorCode valueOf(String name, ErrorCode defaultValue) {
        try {
            return ErrorCode.valueOf(name);
        } catch (IllegalArgumentException e) {
            return defaultValue;
        }
    }

    /**
     * Returns the error code corresponding to a gRPC status, or {@code UNKNOWN} if not recognized.
     */
    static ErrorCode fromGrpcStatus(GrpcStatus status) {
        ErrorCode code = errorByRpcCode.get(status.code);
        return code == null ? UNKNOWN : code;
    }
}
