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

suite("test_cloud_memtable_agg_mor", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(3)
    options.enableDebugPoints()
    options.beConfigs += ['enable_packed_file=true',
                          'small_file_threshold_bytes=1048576']
    options.feConfigs += ['min_bytes_per_broker_scanner=100',
                          'stream_load_default_cloud_memtable_direct_upload=true']
    docker(options) {
        sql "DROP TABLE IF EXISTS cloud_memtable_models_source"
        sql """
            CREATE TABLE cloud_memtable_models_source (n BIGINT NOT NULL)
            DUPLICATE KEY(n) DISTRIBUTED BY HASH(n) BUCKETS 12
            PROPERTIES ("replication_num"="1")
        """
        sql "SET enable_memtable_on_sink_node=false"
        sql "INSERT INTO cloud_memtable_models_source SELECT number FROM numbers('number'='12000')"
        sql "SET enable_memtable_on_sink_node=true"
        sql "SET parallel_pipeline_task_num=4"
        sql "SET profile_level=2"
        sql "SET enable_file_cache=false"

        def aggQuery = """
            SELECT k, s, lo, hi, bitmap_count(b), hll_cardinality(h), r, rn
            FROM cloud_memtable_agg
        """
        def morQuery = "SELECT k, v FROM cloud_memtable_mor"
        def seqQuery = "SELECT k, v, seq FROM cloud_memtable_mor_seq"
        def checkLayout = { table, tag ->
            def tablet = sql_return_maparray("SHOW TABLETS FROM ${table}")[0]
            def partition = sql_return_maparray("SHOW PARTITIONS FROM ${table}")[0]
            def ms = cluster.getAllMetaservices()[0]
            getSegmentFilesFromMs("${ms.host}:${ms.httpPort}", tablet.TabletId, partition.VisibleVersion) {
                code, body ->
                    assertEquals(200, code)
                    def meta = parseJson(body)
                    logger.info("{} rowset layout: {}", table, meta)
                    def ids = meta.segment_ids ?: []
                    quickTest(tag, """
                        SELECT ${meta.segments_overlap_pb == 'OVERLAPPING'},
                            ${ids.size() == (meta.num_segments as int)},
                            ${ids.toSet().size() == ids.size()},
                            ${ids.any { (it as long) >= 1000 }},
                            ${(meta.packed_slice_locations ?: [:]).size() > 0}
                    """, true)
            }
        }
        def loadS3 = { table, direct ->
            def label = "agg_mor_" + UUID.randomUUID().toString().replace('-', '_')
            sql "SET enable_profile=true"
            sql """
                LOAD LABEL ${label} (
                    DATA INFILE("s3://${getS3BucketName()}/regression/load/data/basic_data.csv")
                    INTO TABLE ${table} COLUMNS TERMINATED BY "|" FORMAT AS "CSV"
                    (k, c01, c02, c03, c04, c05, c06, c07, c08, c09,
                        c10, c11, c12, c13, c14, c15, c16, c17, c18)
                    SET (v = k * 2)
                ) WITH S3 (
                    "AWS_ACCESS_KEY"="${getS3AK()}", "AWS_SECRET_KEY"="${getS3SK()}",
                    "AWS_ENDPOINT"="${getS3Endpoint()}", "AWS_REGION"="${getS3Region()}",
                    "provider"="${getS3Provider()}"
                ) PROPERTIES ("load_parallelism"="1")
            """
            waitForBrokerLoadDone(label)
            def required = direct ? ["DeltaWriterV2", "CloudMemtableDirectUpload: true"] : ["DeltaWriterV2"]
            new ProfileAction(context).getProfileBySql(label, required)
            sql "SET enable_profile=false"
        }

        [false, true].each { direct ->
            sql "SET enable_cloud_memtable_direct_upload=${direct}"
            if (direct) {
                GetDebugPoint().enableDebugPointForAllBEs("LoadStreamWriter.append_data.unexpected_transfer")
            }
            sql "DROP TABLE IF EXISTS cloud_memtable_agg"
            sql "DROP TABLE IF EXISTS cloud_memtable_mor"
            sql "DROP TABLE IF EXISTS cloud_memtable_mor_seq"
            sql "DROP TABLE IF EXISTS cloud_memtable_agg_broker"
            sql "DROP TABLE IF EXISTS cloud_memtable_mor_broker"
            sql """
                CREATE TABLE cloud_memtable_agg (
                    k BIGINT NOT NULL, s BIGINT SUM, lo BIGINT MIN, hi BIGINT MAX,
                    b BITMAP BITMAP_UNION, h HLL HLL_UNION,
                    r BIGINT REPLACE, rn BIGINT REPLACE_IF_NOT_NULL
                ) AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true")
            """
            sql """
                CREATE TABLE cloud_memtable_mor (k BIGINT NOT NULL, v BIGINT)
                UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="false",
                            "disable_auto_compaction"="true")
            """
            sql """
                CREATE TABLE cloud_memtable_mor_seq (
                    k BIGINT NOT NULL, v BIGINT, seq BIGINT NOT NULL,
                    INDEX idx_k (k) USING INVERTED
                ) UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="false",
                            "function_column.sequence_col"="seq", "inverted_index_storage_format"="V2",
                            "disable_auto_compaction"="true")
            """
            sql "SET enable_profile=true"
            sql """
                /* cloud_memtable_agg_${direct} */
                INSERT INTO cloud_memtable_agg
                SELECT n % 4, n, n, n, to_bitmap(n), hll_hash(CAST(n % 4 AS STRING)),
                    n % 4 + 10, IF(n % 8 < 4, n % 4 + 20, NULL)
                FROM cloud_memtable_models_source
            """
            sql """
                /* cloud_memtable_mor_${direct} */
                INSERT INTO cloud_memtable_mor SELECT n % 4, n % 4 * 10 FROM cloud_memtable_models_source
            """
            sql """
                /* cloud_memtable_mor_seq_${direct} */
                INSERT INTO cloud_memtable_mor_seq SELECT n % 4, n * 10, n FROM cloud_memtable_models_source
            """
            ['agg', 'mor', 'mor_seq'].each { model ->
                def required = direct ? ["DeltaWriterV2", "CloudMemtableDirectUpload: true"] : ["DeltaWriterV2"]
                new ProfileAction(context).getProfileBySql("cloud_memtable_${model}_${direct}", required)
                if (direct) {
                    checkLayout("cloud_memtable_${model}", "layout_${model}")
                }
            }
            sql "SET enable_profile=false"
            quickTest("agg_${direct}", aggQuery, true)
            quickTest("mor_${direct}", morQuery, true)
            quickTest("seq_${direct}", seqQuery, true)
            quickTest("seq_index_${direct}", seqQuery + " WHERE k IN (1,3)", true)

            // A newer transaction with a lower Sequence must not replace the business-newer row.
            sql "INSERT INTO cloud_memtable_mor_seq VALUES (0,-1,1)"
            quickTest("seq_lower_${direct}", seqQuery, true)
            sql "INSERT INTO cloud_memtable_mor VALUES (0,99)"
            quickTest("mor_new_version_${direct}", morQuery, true)
            sql """
                INSERT INTO cloud_memtable_agg
                VALUES (0,10,-1,13000,bitmap_empty(),hll_empty(),99,NULL)
            """
            quickTest("agg_new_version_${direct}", aggQuery, true)
            sql "INSERT INTO cloud_memtable_mor_seq SELECT n % 4,n*10,n FROM cloud_memtable_models_source WHERE n<0"
            quickTest("seq_empty_${direct}", seqQuery, true)
            ['agg', 'mor', 'mor_seq'].each { model ->
                trigger_and_wait_compaction("cloud_memtable_${model}", "full")
            }
            quickTest("agg_compacted_${direct}", aggQuery, true)
            quickTest("mor_compacted_${direct}", morQuery, true)
            quickTest("seq_compacted_${direct}", seqQuery, true)

            sql """
                CREATE TABLE cloud_memtable_agg_broker (k BIGINT NOT NULL, v BIGINT SUM)
                AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true")
            """
            sql """
                CREATE TABLE cloud_memtable_mor_broker (k BIGINT NOT NULL, v BIGINT)
                UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="false",
                            "disable_auto_compaction"="true")
            """
            loadS3("cloud_memtable_agg_broker", direct)
            loadS3("cloud_memtable_mor_broker", direct)
            quickTest("agg_broker_${direct}", "SELECT k,v FROM cloud_memtable_agg_broker", true)
            quickTest("mor_broker_${direct}", "SELECT k,v FROM cloud_memtable_mor_broker", true)
        }

        streamLoad {
            table "cloud_memtable_agg_broker"
            set "column_separator", ","
            set "memtable_on_sink_node", "true"
            inputStream new ByteArrayInputStream("50,1\n50,2\n".getBytes())
            check { result, exception, startTime, endTime ->
                if (exception != null) { throw exception }
                assertEquals("Success", parseJson(result).Status)
            }
        }
        order_qt_agg_stream "SELECT k,v FROM cloud_memtable_agg_broker WHERE k=50"
        streamLoad {
            table "cloud_memtable_mor_seq"
            set "column_separator", ","
            set "memtable_on_sink_node", "true"
            inputStream new ByteArrayInputStream("0,1,1\n1,130010,13001\n".getBytes())
            check { result, exception, startTime, endTime ->
                if (exception != null) { throw exception }
                assertEquals("Success", parseJson(result).Status)
            }
        }
        order_qt_mor_stream seqQuery
        sql "INSERT INTO cloud_memtable_mor_seq (k,v,seq,__DORIS_DELETE_SIGN__) VALUES (2,140000,14000,1)"
        order_qt_mor_delete seqQuery
        sql "INSERT INTO cloud_memtable_mor_seq VALUES (2,150000,15000)"
        order_qt_mor_reinsert seqQuery
        trigger_and_wait_compaction("cloud_memtable_mor_seq", "full")
        order_qt_mor_final_compacted seqQuery

        // MOW must keep the established CloudDeltaWriter path even with both switches enabled.
        sql "DROP TABLE IF EXISTS cloud_memtable_mow_fallback"
        sql """
            CREATE TABLE cloud_memtable_mow_fallback (k BIGINT NOT NULL, v BIGINT)
            UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true")
        """
        sql "INSERT INTO cloud_memtable_mow_fallback VALUES (1,10)"
        sql "INSERT INTO cloud_memtable_mow_fallback VALUES (1,20)"
        order_qt_mow_fallback "SELECT * FROM cloud_memtable_mow_fallback"
    }
}
