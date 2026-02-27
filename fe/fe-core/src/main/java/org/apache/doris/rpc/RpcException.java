// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.rpc;

import com.google.common.base.Strings;

public class RpcException extends Exception {

    /**
     * Categorizes the type of RPC failure for adaptive throttling decisions.
     */
    public enum FailureType {
        /** RPC timed out (gRPC DEADLINE_EXCEEDED) */
        TIMEOUT,
        /** Other failures (UNAVAILABLE, UNKNOWN, etc.) */
        OTHER
    }

    private String host;
    private FailureType failureType;

    public RpcException(String host, String message) {
        super(message);
        this.host = host;
        this.failureType = FailureType.OTHER;
    }

    public RpcException(String host, String message, Exception e) {
        super(message, e);
        this.host = host;
        this.failureType = FailureType.OTHER;
    }

    public RpcException(String host, String message, Exception e, FailureType failureType) {
        super(message, e);
        this.host = host;
        this.failureType = failureType;
    }

    public FailureType getFailureType() {
        return failureType;
    }

    public boolean isTimeout() {
        return failureType == FailureType.TIMEOUT;
    }

    @Override
    public String getMessage() {
        if (Strings.isNullOrEmpty(host)) {
            return super.getMessage();
        }
        return super.getMessage() + ", host: " + host;
    }
}
