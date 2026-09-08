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

package org.apache.doris.alter;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.task.AlterReplicaTask;
import org.apache.doris.thrift.TAlterTabletReqV2;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TResourceLimit;

import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;

public class AlterJobV2SerializationTest {
    @Test
    public void testQueryContextPersistence() throws Exception {
        for (AlterJobV2 job : List.of(new SchemaChangeJobV2(), new RollupJobV2())) {
            job.jobState = AlterJobV2.JobState.WAITING_TXN;
            job.queryGlobals = new TQueryGlobals("2026-09-08 11:36:30")
                    .setTimestampMs(1788838590000L)
                    .setTimeZone("Asia/Shanghai")
                    .setNanoSeconds(123456789)
                    .setLoadZeroTolerance(false)
                    .setLcTimeNames("en_US");
            job.queryOptions = new TQueryOptions()
                    .setMemLimit(123456789L)
                    .setQueryTimeout(37)
                    .setResourceLimit(new TResourceLimit().setCpuLimit(2));
            // A primitive's stored value must not turn an unset option into a set option on replay.
            job.queryOptions.unsetQueryTimeout();

            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                job.write(out);
            }
            AlterJobV2 replayed;
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                replayed = AlterJobV2.read(in);
            }
            Assertions.assertEquals(job.getType(), replayed.getType());
            Assertions.assertEquals(job.getJobState(), replayed.getJobState());
            Assertions.assertEquals(job.queryGlobals, replayed.queryGlobals);
            Assertions.assertEquals(job.queryOptions, replayed.queryOptions);
            Assertions.assertTrue(replayed.queryGlobals.isSetLoadZeroTolerance());
            Assertions.assertFalse(replayed.queryOptions.isSetQueryTimeout());

            TAlterTabletReqV2 request = toThrift(replayed);
            Assertions.assertTrue(request.isSetQueryGlobals());
            Assertions.assertTrue(request.isSetQueryOptions());
            TSerializer serializer = new TSerializer();
            Assertions.assertArrayEquals(serializer.serialize(toThrift(job)), serializer.serialize(request));
        }
    }

    @Test
    public void testLegacyQueryContext() throws Exception {
        for (String jobClass : List.of("SchemaChangeJobV2", "RollupJobV2",
                "CloudSchemaChangeJobV2", "CloudRollupJobV2")) {
            String type = jobClass.contains("SchemaChange") ? "SCHEMA_CHANGE" : "ROLLUP";
            for (String context : List.of("", ",\"queryGlobals\":{},\"queryOptions\":{}")) {
                String json = "{\"clazz\":\"" + jobClass + "\",\"type\":\"" + type
                        + "\",\"jobState\":\"WAITING_TXN\"" + context + "}";
                AlterJobV2 job = GsonUtils.GSON.fromJson(json, AlterJobV2.class);
                TAlterTabletReqV2 request = toThrift(job);
                Assertions.assertFalse(request.isSetQueryGlobals());
                Assertions.assertFalse(request.isSetQueryOptions());
                // Validate the complete outgoing request, including Thrift's required fields.
                new TSerializer().serialize(request);
            }
        }
    }

    private TAlterTabletReqV2 toThrift(AlterJobV2 job) {
        DescriptorTable descTable = new DescriptorTable();
        descTable.createTupleDescriptor();
        return new AlterReplicaTask(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
                job.getType(), null, descTable, null, null, null, 0, "",
                job.queryOptions, job.queryGlobals).toThrift();
    }
}
