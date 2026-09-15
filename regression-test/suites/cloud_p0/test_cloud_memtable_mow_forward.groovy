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

import org.apache.doris.regression.action.ProfileAction
import org.apache.doris.regression.suite.ClusterOptions

suite("test_cloud_memtable_mow_forward", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(3)
    options.enableDebugPoints()
    options.beConfigs += ['small_file_threshold_bytes=1048576',
                          'enable_merge_on_write_correctness_check=true']
    options.feConfigs += ['stream_load_default_memtable_on_sink_node=true',
                          'stream_load_default_cloud_memtable_sink_upload=false']
    docker(options) {
        sql "DROP TABLE IF EXISTS cloud_mow_forward_source"
        sql """
            CREATE TABLE cloud_mow_forward_source (n BIGINT NOT NULL)
            DUPLICATE KEY(n) DISTRIBUTED BY HASH(n) BUCKETS 12
            PROPERTIES ("replication_num"="1")
        """
        sql "SET enable_memtable_on_sink_node=false"
        sql "INSERT INTO cloud_mow_forward_source SELECT number FROM numbers('number'='12000')"
        sql "SET enable_cloud_memtable_sink_upload=false"
        sql "SET parallel_pipeline_task_num=4"
        sql "SET profile_level=2"
        sql "SET enable_sql_cache=false"
        // Any accidental switch to sink upload must fail before its result is committed.
        GetDebugPoint().enableDebugPointForAllBEs("DeltaWriterV2.sink_upload.after_upload_failure")
        try {
            [false, true].each { packed ->
                setBeConfigTemporary(['enable_packed_file': packed.toString()]) {
                    sql "DROP TABLE IF EXISTS cloud_mow_forward"
                    sql """
                        CREATE TABLE cloud_mow_forward (
                            k BIGINT NOT NULL, v BIGINT, seq BIGINT NOT NULL,
                            INDEX idx_k(k) USING INVERTED
                        ) UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                        PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                                    "function_column.sequence_col"="seq", "inverted_index_storage_format"="V2",
                                    "disable_auto_compaction"="true", "group_commit_interval_ms"="200")
                    """
                    sql "SET enable_memtable_on_sink_node=false"
                    sql "INSERT INTO cloud_mow_forward VALUES (0,-1,-1),(1,-1,-1),(2,-1,-1),(3,-1,-1)"
                    sql "INSERT INTO cloud_mow_forward VALUES (0,0,0),(1,0,0),(2,0,0),(3,0,0)"
                    sql "SET enable_memtable_on_sink_node=true"
                    sql "SET enable_profile=true"
                    sql """
                        /* cloud_mow_forward_${packed} */
                        INSERT INTO cloud_mow_forward SELECT n%4,n*10,n+1 FROM cloud_mow_forward_source
                    """
                    new ProfileAction(context).getProfileBySql("cloud_mow_forward_${packed}", ["DeltaWriterV2"])
                    sql "SET enable_profile=false"
                    quickTest("initial_${packed}", "SELECT * FROM cloud_mow_forward", true)
                    quickTest("index_${packed}", "SELECT * FROM cloud_mow_forward WHERE k IN (1,3)", true)
                    def tablet = sql_return_maparray("SHOW TABLETS FROM cloud_mow_forward")[0]
                    def partition = sql_return_maparray("SHOW PARTITIONS FROM cloud_mow_forward")[0]
                    def ms = cluster.getAllMetaservices()[0]
                    getSegmentFilesFromMs("${ms.host}:${ms.httpPort}", tablet.TabletId, partition.VisibleVersion) {
                        code, body ->
                            assertEquals(200, code)
                            def meta = parseJson(body)
                            quickTest("layout_${packed}", """
                                SELECT ${meta.num_segments as int} > 1,
                                    ${(meta.packed_slice_locations ?: [:]).size() > 0},
                                    ${meta.segments_file_size.size() == (meta.num_segments as int)},
                                    ${meta.segments_file_size.every { (it as long) > 0 }},
                                    ${meta.inverted_index_file_info.size() == (meta.num_segments as int)},
                                    ${meta.inverted_index_file_info.every { (it.index_size as long) > 0 }}
                            """, true)
                    }
                    sql "INSERT INTO cloud_mow_forward VALUES (0,-1,1)"
                    sql "INSERT INTO cloud_mow_forward (k,v,seq,__DORIS_DELETE_SIGN__) VALUES (2,130000,13000,1)"
                    sql "INSERT INTO cloud_mow_forward VALUES (2,-1,1)"
                    quickTest("deleted_${packed}", "SELECT * FROM cloud_mow_forward", true)
                    sql "INSERT INTO cloud_mow_forward VALUES (2,140000,14000)"
                    streamLoad {
                        table "cloud_mow_forward"
                        set "column_separator", ","
                        set "memtable_on_sink_node", "true"
                        set "group_commit", "off_mode"
                        inputText "0,150000,15000\n3,-1,1\n"
                        check { result, exception, startTime, endTime ->
                            if (exception != null) { throw exception }
                            def response = parseJson(result)
                            quickTest("stream_status_${packed}", "SELECT '${response.Status}'", true)
                        }
                    }
                    quickTest("stream_${packed}", "SELECT * FROM cloud_mow_forward", true)
                    streamLoad {
                        table "cloud_mow_forward"
                        set "column_separator", ","
                        set "group_commit", "async_mode"
                        unset "label"
                        inputText "1,160000,16000\n"
                        check { result, exception, startTime, endTime ->
                            if (exception != null) { throw exception }
                            def response = parseJson(result)
                            quickTest("group_status_${packed}", "SELECT '${response.Status}', '${response.GroupCommit}'", true)
                        }
                    }
                    awaitUntil(60) {
                        (sql "SELECT v FROM cloud_mow_forward WHERE k=1")[0][0] == 160000
                    }
                    quickTest("group_${packed}", "SELECT * FROM cloud_mow_forward", true)
                    trigger_and_wait_compaction("cloud_mow_forward", "full")
                    quickTest("compacted_${packed}", "SELECT * FROM cloud_mow_forward", true)
                }
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("DeltaWriterV2.sink_upload.after_upload_failure")
        }
    }
}
