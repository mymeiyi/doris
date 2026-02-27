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

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.OptionalInt;

public class MetaServiceCodeExtractorTest {

    @After
    public void tearDown() {
        MetaServiceCodeExtractor.clearCache();
    }

    @Test
    public void testTryGetCodeWithOkResponse() {
        Cloud.GetVersionResponse response = Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK)
                        .build())
                .build();

        OptionalInt code = MetaServiceCodeExtractor.tryGetCode(response);
        Assert.assertTrue(code.isPresent());
        Assert.assertEquals(Cloud.MetaServiceCode.OK_VALUE, code.getAsInt());
    }

    @Test
    public void testTryGetCodeWithMaxQpsLimit() {
        Cloud.GetVersionResponse response = Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.MAX_QPS_LIMIT)
                        .build())
                .build();

        OptionalInt code = MetaServiceCodeExtractor.tryGetCode(response);
        Assert.assertTrue(code.isPresent());
        Assert.assertEquals(Cloud.MetaServiceCode.MAX_QPS_LIMIT_VALUE, code.getAsInt());
    }

    @Test
    public void testIsMaxQpsLimitReturnsTrue() {
        Cloud.GetVersionResponse response = Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.MAX_QPS_LIMIT)
                        .build())
                .build();

        Assert.assertTrue(MetaServiceCodeExtractor.isMaxQpsLimit(response));
    }

    @Test
    public void testIsMaxQpsLimitReturnsFalseForOk() {
        Cloud.GetVersionResponse response = Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK)
                        .build())
                .build();

        Assert.assertFalse(MetaServiceCodeExtractor.isMaxQpsLimit(response));
    }

    @Test
    public void testTryGetCodeWithNullResponse() {
        OptionalInt code = MetaServiceCodeExtractor.tryGetCode(null);
        Assert.assertFalse(code.isPresent());
    }

    @Test
    public void testTryGetCodeWithNonProtobufObject() {
        OptionalInt code = MetaServiceCodeExtractor.tryGetCode("not a protobuf");
        Assert.assertFalse(code.isPresent());
    }

    @Test
    public void testTryGetCodeWithNoStatusSet() {
        Cloud.GetVersionResponse response = Cloud.GetVersionResponse.newBuilder().build();

        OptionalInt code = MetaServiceCodeExtractor.tryGetCode(response);
        Assert.assertFalse(code.isPresent());
    }

    @Test
    public void testCacheIsPopulatedOnSecondCall() {
        Cloud.GetVersionResponse response = Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK)
                        .build())
                .build();

        OptionalInt code1 = MetaServiceCodeExtractor.tryGetCode(response);
        OptionalInt code2 = MetaServiceCodeExtractor.tryGetCode(response);

        Assert.assertTrue(code1.isPresent());
        Assert.assertTrue(code2.isPresent());
        Assert.assertEquals(code1.getAsInt(), code2.getAsInt());
    }

    @Test
    public void testDifferentResponseTypes() {
        Cloud.GetVersionResponse versionResponse = Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK)
                        .build())
                .build();

        Cloud.CreateTabletsResponse tabletsResponse = Cloud.CreateTabletsResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.MAX_QPS_LIMIT)
                        .build())
                .build();

        OptionalInt code1 = MetaServiceCodeExtractor.tryGetCode(versionResponse);
        OptionalInt code2 = MetaServiceCodeExtractor.tryGetCode(tabletsResponse);

        Assert.assertTrue(code1.isPresent());
        Assert.assertEquals(Cloud.MetaServiceCode.OK_VALUE, code1.getAsInt());
        Assert.assertTrue(code2.isPresent());
        Assert.assertEquals(Cloud.MetaServiceCode.MAX_QPS_LIMIT_VALUE, code2.getAsInt());
    }

    @Test
    public void testClearCacheAllowsRepopulation() {
        Cloud.GetVersionResponse response = Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK)
                        .build())
                .build();

        MetaServiceCodeExtractor.tryGetCode(response);
        MetaServiceCodeExtractor.clearCache();

        OptionalInt code = MetaServiceCodeExtractor.tryGetCode(response);
        Assert.assertTrue(code.isPresent());
        Assert.assertEquals(Cloud.MetaServiceCode.OK_VALUE, code.getAsInt());
    }
}
