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

suite("test_cloud_duplicate_memtable_on_sink", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(3)
    options.enableDebugPoints()
    // Split the small S3 CSV across all three BEs with load_parallelism = 1.
    options.feConfigs += ['min_bytes_per_broker_scanner = 100']
    options.beConfigs += ['enable_packed_file=true', 'small_file_threshold_bytes=1048576']

    options.feConfigs += ['stream_load_default_cloud_memtable_direct_upload = true']
    docker(options) {
        [false, true].each { directUpload ->
            sql "SET enable_cloud_memtable_direct_upload = ${directUpload}"
            if (directUpload) {
                GetDebugPoint().enableDebugPointForAllBEs("LoadStreamWriter.append_data.unexpected_transfer")
                GetDebugPoint().enableDebugPointForAllBEs("DeltaWriterV2.direct_upload.duplicate_result")
            }

            sql "SET enable_sql_cache = false"
            sql "DROP TABLE IF EXISTS test_cloud_duplicate_memtable_on_sink_source"
            sql "DROP TABLE IF EXISTS test_cloud_duplicate_memtable_on_sink"

            sql """
                CREATE TABLE test_cloud_duplicate_memtable_on_sink_source (
                    k BIGINT NOT NULL,
                    v BIGINT NOT NULL
                )
                DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            sql """
                CREATE TABLE test_cloud_duplicate_memtable_on_sink (
                    k BIGINT NOT NULL,
                    v BIGINT NOT NULL
                )
                DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """

            sql "SET enable_memtable_on_sink_node = false"
            sql """
                INSERT INTO test_cloud_duplicate_memtable_on_sink_source
                SELECT number, number * 2 FROM numbers("number" = "100000")
            """

            try {
                sql "SET enable_memtable_on_sink_node = true"
                sql "SET profile_level = 2"
                sql "SET enable_profile = true"
                sql """
                    /* cloud_duplicate_memtable_on_sink_profile_${directUpload} */
                    INSERT INTO test_cloud_duplicate_memtable_on_sink
                    SELECT k, v FROM test_cloud_duplicate_memtable_on_sink_source
                """
                def required = ["DeltaWriterV2"]
                if (directUpload) {
                    required += "CloudMemtableDirectUpload: true"
                }
                def profileString = new ProfileAction(context).getProfileBySql(
                        "cloud_duplicate_memtable_on_sink_profile_${directUpload}", required)
                logger.info("memtable-on-sink profile:\n{}", profileString)
            } finally {
                sql "SET enable_profile = false"
                sql "SET enable_memtable_on_sink_node = false"
            }

            sql """
                SELECT assert_true(
                    COUNT(*) = 100000
                        AND SUM(k) = 4999950000
                        AND SUM(v) = 9999900000,
                    'cloud duplicate memtable-on-sink result mismatch')
                FROM test_cloud_duplicate_memtable_on_sink
            """

            sql "DROP TABLE IF EXISTS test_cloud_duplicate_memtable_on_sink_s3"
            sql """
                CREATE TABLE test_cloud_duplicate_memtable_on_sink_s3 (
                    k BIGINT NOT NULL,
                    v BIGINT NOT NULL,
                    INDEX idx_v (v) USING INVERTED
                )
                DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "inverted_index_storage_format" = "V2",
                    "disable_auto_compaction" = "true"
                )
            """

            def label = "cloud_duplicate_memtable_on_sink_s3_" + UUID.randomUUID().toString().replace('-', '_')
            try {
                sql "SET enable_memtable_on_sink_node = true"
                sql "SET enable_profile = true"
                // The three scanners read 6, 6, and 8 rows. Flush after the first four rows on
                // each BE, then flush the remaining rows at close to produce two segments each.
                sql "SET broker_load_batch_size = 4"
                GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush", [execute: 1])
                sql """
                    LOAD LABEL ${label} (
                        DATA INFILE("s3://${getS3BucketName()}/regression/load/data/basic_data.csv")
                        INTO TABLE test_cloud_duplicate_memtable_on_sink_s3
                        COLUMNS TERMINATED BY "|"
                        FORMAT AS "CSV"
                        (k, c01, c02, c03, c04, c05, c06, c07, c08, c09,
                            c10, c11, c12, c13, c14, c15, c16, c17, c18)
                        SET (v = k * 2)
                    )
                    WITH S3 (
                        "AWS_ACCESS_KEY" = "${getS3AK()}",
                        "AWS_SECRET_KEY" = "${getS3SK()}",
                        "AWS_ENDPOINT" = "${getS3Endpoint()}",
                        "AWS_REGION" = "${getS3Region()}",
                        "provider" = "${getS3Provider()}"
                    )
                    PROPERTIES ("load_parallelism" = "1")
                """
                waitForBrokerLoadDone(label)
                def load = sql_return_maparray("SHOW LOAD WHERE LABEL = '${label}'")[0]
                assertEquals("FINISHED", load.State, "S3 load did not finish: ${load}")

                def profileString = new ProfileAction(context).getProfileBySql(
                        label, ["DeltaWriterV2", "NumScanners"])
                logger.info("S3 memtable-on-sink profile:\n{}", profileString)

                // Only inspect per-BE pipelines, excluding the merged profile's duplicate counters.
                def pipelines = profileString.split(/(?m)(?=^[ \t]*(?:Pipeline \d+|FragmentLevelProfile:)\(host=)/)
                        .findAll { it.trim().startsWith("Pipeline ") }
                def backends = sql_return_maparray("SHOW BACKENDS")
                backends.each { backend ->
                    def pipeline = pipelines.find {
                        it.readLines()[0].contains("hostname:${backend.Host},") && it.contains("DeltaWriterV2")
                    }
                    assertNotNull(pipeline, "Missing S3 sink writer on BE ${backend.Host}")
                    assertTrue(pipeline.contains("FILE_SCAN_OPERATOR"), "Missing S3 scanner on BE ${backend.Host}")
                    assertTrue((pipeline =~ /(?m)^\s*- NumScanners: 1\s*$/).find(),
                            "Expected one S3 scanner on BE ${backend.Host}")
                    assertTrue((pipeline =~ /(?m)^\s*- SegmentNum: 2\s*$/).find(),
                            "Expected two flushed segments on BE ${backend.Host}")
                }
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs("MemTable.need_flush")
                sql "SET enable_profile = false"
                sql "SET enable_memtable_on_sink_node = false"
            }

            // Read both segment data and V2 indexes without the uploader's file cache.
            sql "SET enable_file_cache = false"
            quickTest("s3_rows_${directUpload}", """
                SELECT COUNT(*), SUM(k), SUM(v)
                FROM test_cloud_duplicate_memtable_on_sink_s3
            """, true)
            quickTest("s3_index_${directUpload}", """
                SELECT k, v FROM test_cloud_duplicate_memtable_on_sink_s3 WHERE v IN (62, 100, 114)
            """, true)
            def tablet = sql_return_maparray("SHOW TABLETS FROM test_cloud_duplicate_memtable_on_sink_s3")[0]
            def partition = sql_return_maparray("SHOW PARTITIONS FROM test_cloud_duplicate_memtable_on_sink_s3")[0]
            def ms = cluster.getAllMetaservices()[0]
            getSegmentFilesFromMs("${ms.host}:${ms.httpPort}", tablet.TabletId, partition.VisibleVersion) {
                responseCode, body ->
                    assertEquals(200, responseCode)
                    logger.info("S3 memtable-on-sink rowset meta: {}", body)
                    def rowsetMeta = parseJson(body)
                    def locations = rowsetMeta.packed_slice_locations
                    def segmentIds = rowsetMeta.segment_ids ?: (0..<(rowsetMeta.num_segments as int)).toList()
                    logger.info("S3 rowset layout (directUpload={}): {}", directUpload, [
                        tablet_id: tablet.TabletId, version: partition.VisibleVersion,
                        rowset_id: rowsetMeta.rowset_id_v2, segment_ids: segmentIds,
                        num_segment_rows: rowsetMeta.num_segment_rows,
                        segments_file_size: rowsetMeta.segments_file_size,
                        packed_files: locations.keySet().sort()
                    ])
                    // Only the destination rowset's first segment and its V2 index are packed.
                    // The other five segments retain independent files.
                    quickTest("s3_packed_meta_${directUpload}", """
                        SELECT ${rowsetMeta.num_segments as int}, ${rowsetMeta.num_rows as long},
                            ${locations.size()},
                            ${locations.keySet().count { it.endsWith('_0.dat') }},
                            ${locations.keySet().count { it.endsWith('_0.idx') }},
                            ${segmentIds.count { (it as long) >= 1000 }}, '${segmentIds.join(",")}'
                    """, true)
            }
        }

        GetDebugPoint().disableDebugPointForAllBEs("DeltaWriterV2.direct_upload.duplicate_result")

        // The stream-load planner propagates the FE default independently of SQL session options.
        streamLoad {
            table "test_cloud_duplicate_memtable_on_sink_s3"
            set "column_separator", ","
            set "memtable_on_sink_node", "true"
            inputStream new ByteArrayInputStream("100,200\n101,202\n".getBytes())
            time 30000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                assertEquals("Success", parseJson(result).Status)
            }
        }
        order_qt_stream_rows """
            SELECT COUNT(*), SUM(k), SUM(v) FROM test_cloud_duplicate_memtable_on_sink_s3
        """

        sql "SET enable_memtable_on_sink_node = true"
        sql "SET enable_cloud_memtable_direct_upload = true"
        // An uploaded partial rowset must remain invisible if its result cannot be sent.
        try {
            GetDebugPoint().enableDebugPointForAllBEs("DeltaWriterV2.direct_upload.after_upload_failure")
            test {
                sql "INSERT INTO test_cloud_duplicate_memtable_on_sink_s3 VALUES (999, 1998)"
                exception "injected failure after direct upload"
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("DeltaWriterV2.direct_upload.after_upload_failure")
        }
        order_qt_failed_upload_rows """
            SELECT COUNT(*), SUM(k), SUM(v) FROM test_cloud_duplicate_memtable_on_sink_s3
        """
    }
}
