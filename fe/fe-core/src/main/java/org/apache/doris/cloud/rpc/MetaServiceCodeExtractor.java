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

package org.apache.doris.cloud.rpc;

import org.apache.doris.cloud.proto.Cloud;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Extracts {@link Cloud.MetaServiceCode} from generic protobuf response objects
 * using cached {@link MethodHandle}s for reflection-based access.
 *
 * <p>All protobuf response messages in cloud.proto share the pattern:
 * {@code optional MetaServiceResponseStatus status = 1}, where
 * {@code MetaServiceResponseStatus} contains {@code optional MetaServiceCode code = 1}.
 *
 * <p>This class caches the method handles per response class so subsequent calls
 * avoid repeated reflection lookups. It is designed to be fail-open: if extraction
 * fails for any reason, an empty {@link OptionalInt} is returned rather than throwing.
 */
public class MetaServiceCodeExtractor {
    private static final Logger LOG = LogManager.getLogger(MetaServiceCodeExtractor.class);

    private static final ConcurrentHashMap<Class<?>, MethodHandle[]> CACHE = new ConcurrentHashMap<>();

    private MetaServiceCodeExtractor() {
    }

    /**
     * Try to extract the MetaServiceCode integer value from a protobuf response.
     *
     * @param response a protobuf response object (e.g., Cloud.GetVersionResponse)
     * @return the code's number if extraction succeeds, or empty if the response is null,
     *         doesn't have the expected methods, or the status/code is not set
     */
    public static OptionalInt tryGetCode(Object response) {
        if (response == null) {
            return OptionalInt.empty();
        }

        try {
            MethodHandle[] handles = CACHE.computeIfAbsent(response.getClass(),
                    MetaServiceCodeExtractor::buildHandles);
            if (handles == null) {
                return OptionalInt.empty();
            }

            boolean hasStatus = (boolean) handles[0].invoke(response);
            if (!hasStatus) {
                return OptionalInt.empty();
            }

            Object status = handles[1].invoke(response);
            if (status == null) {
                return OptionalInt.empty();
            }

            Object code = handles[2].invoke(status);
            if (code instanceof Cloud.MetaServiceCode) {
                return OptionalInt.of(((Cloud.MetaServiceCode) code).getNumber());
            }

            return OptionalInt.empty();
        } catch (Throwable t) {
            LOG.debug("Failed to extract MetaServiceCode from {}: {}", response.getClass().getSimpleName(),
                    t.getMessage());
            return OptionalInt.empty();
        }
    }

    /**
     * Check if the response's MetaServiceCode is MAX_QPS_LIMIT.
     *
     * @param response a protobuf response object
     * @return true if the code is MAX_QPS_LIMIT (6001)
     */
    public static boolean isMaxQpsLimit(Object response) {
        OptionalInt code = tryGetCode(response);
        return code.isPresent() && code.getAsInt() == Cloud.MetaServiceCode.MAX_QPS_LIMIT_VALUE;
    }

    private static MethodHandle[] buildHandles(Class<?> clazz) {
        try {
            MethodHandles.Lookup lookup = MethodHandles.publicLookup();

            MethodHandle hasStatus = lookup.findVirtual(clazz, "hasStatus", MethodType.methodType(boolean.class));

            MethodHandle getStatus = lookup.findVirtual(clazz, "getStatus",
                    MethodType.methodType(Cloud.MetaServiceResponseStatus.class));

            MethodHandle getCode = lookup.findVirtual(Cloud.MetaServiceResponseStatus.class, "getCode",
                    MethodType.methodType(Cloud.MetaServiceCode.class));

            return new MethodHandle[] {hasStatus, getStatus, getCode};
        } catch (NoSuchMethodException | IllegalAccessException e) {
            LOG.warn("Cannot build method handles for {}: {}. "
                    + "This response type may not have a status field.", clazz.getSimpleName(), e.getMessage());
            return null;
        }
    }

    @VisibleForTesting
    public static void clearCache() {
        CACHE.clear();
    }
}
