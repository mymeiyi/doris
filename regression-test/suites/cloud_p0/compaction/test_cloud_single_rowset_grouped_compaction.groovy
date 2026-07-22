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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_cloud_single_rowset_grouped_compaction", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.enableDebugPoints()
    int initialInputSegmentsPerGroup = 2
    long compactionTimeoutMs = 90000L
    options.beConfigs += [
        "doris_scanner_row_bytes=1",
        "enable_cloud_single_rowset_compaction=true",
        "cloud_single_rowset_compaction_min_segments=2",
        "cloud_single_rowset_compaction_segment_group_size=${initialInputSegmentsPerGroup}",
        "disable_auto_compaction=true",
        "enable_java_support=false"
    ]

    docker(options) {
        def showTablet = { tableName, beHost, bePort, tabletId ->
            sql "SELECT COUNT(*) FROM ${tableName}"
            def (code, out, err) = be_show_tablet_status(beHost, bePort, tabletId)
            logger.info("Show tablet status: code=${code}, out=${out}, err=${err}")
            assertEquals(0, code)
            return parseJson(out.trim())
        }

        def rowsetByVersion = { tabletJson, int version ->
            def rowset = tabletJson.rowsets.find { it.startsWith("[${version}-${version}] ") }
            assertNotNull(rowset)
            return rowset
        }

        def parseRowsetInfo = { rowset ->
            def matcher = rowset =~ /\[[0-9]+-[0-9]+\]\s+([0-9]+)\s+DATA\s+([A-Z_]+)/
            assertTrue(matcher.find(), "unexpected rowset format: ${rowset}")
            return [segments: matcher.group(1).toInteger(), overlap: matcher.group(2)]
        }

        def waitForCompaction = { beHost, bePort, tabletId, timeoutMs ->
            long deadline = System.currentTimeMillis() + timeoutMs
            def lastStatus = null
            while (System.currentTimeMillis() < deadline) {
                def (code, out, err) = be_get_compaction_status(beHost, bePort, tabletId)
                logger.info("Get compaction status: code=${code}, out=${out}, err=${err}")
                assertEquals(0, code)
                lastStatus = parseJson(out.trim())
                assertEquals("success", lastStatus.status.toLowerCase())
                if (!lastStatus.run_status) {
                    return
                }
                Thread.sleep(1000)
            }
            assertTrue(false, "compaction did not finish on ${beHost}:${bePort}, " +
                    "tablet=${tabletId}, timeoutMs=${timeoutMs}, last=${lastStatus}")
        }

        def runCumulativeCompaction = { beHost, bePort, tabletId ->
            def (code, out, err) = be_run_cumulative_compaction(beHost, bePort, tabletId)
            logger.info("Run compaction: code=${code}, out=${out}, err=${err}")
            assertEquals(0, code)
            def compactJson = parseJson(out.trim())
            assertEquals("success", compactJson.status.toLowerCase())
            waitForCompaction(beHost, bePort, tabletId, compactionTimeoutMs)
        }

        def readPointRows = { String tableName ->
            def pointResult = sql "SELECT k, v FROM ${tableName} WHERE k = 100 ORDER BY v"
            return pointResult.collect { row -> row.collect { column -> column.toString() } }
        }

        def checkPointRows = { String tableName, List<List<String>> expectedPointRows ->
            def pointRows = readPointRows(tableName)
            if (expectedPointRows != null) {
                assertEquals(expectedPointRows, pointRows)
            }
            return pointRows
        }

        def checkSingleRowsetGroupedCompaction = {
                String tableName, String keyType, String valueColumn, String extraProperties,
                int inputSegmentsPerGroup, int rowsPerLoadRound, int expectedRows,
                List<List<String>> expectedPointRows,
                boolean expectMultipleOutputSegmentsPerGroup ->
            sql "DROP TABLE IF EXISTS ${tableName}"
            sql """
                CREATE TABLE ${tableName} (
                    k INT,
                    ${valueColumn}
                )
                ${keyType}(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true"${extraProperties}
                )
            """

            StringBuilder content = new StringBuilder()
            for (int i = 0; i < 2; i++) {
                (1..rowsPerLoadRound).each {
                    content.append("${it},${it + i}\n")
                }
            }
            streamLoad {
                table "${tableName}"
                set "column_separator", ","
                inputStream new ByteArrayInputStream(content.toString().getBytes())
                time 30000
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(rowsPerLoadRound * 2, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            sql "sync"

            def pointRowsBeforeCompaction = checkPointRows(tableName, expectedPointRows)
            if (expectedPointRows == null) {
                assertEquals(1, pointRowsBeforeCompaction.size())
            }

            def tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
            assertEquals(1, tablets.size())
            def tabletId = tablets[0].TabletId
            def backendId = tablets[0].BackendId
            def backends = sql_return_maparray "SHOW BACKENDS"
            def backend = backends.find { it.BackendId == backendId }
            assertNotNull(backend)

            def before = showTablet(tableName, backend.Host, backend.HttpPort, tabletId)
            def inputRowset = rowsetByVersion(before, 2)
            def inputInfo = parseRowsetInfo(inputRowset)
            assertEquals("OVERLAPPING", inputInfo.overlap)
            assertTrue(inputInfo.segments > inputSegmentsPerGroup, inputRowset)

            set_be_param("cloud_single_rowset_compaction_segment_group_size",
                    inputSegmentsPerGroup.toString())
            runCumulativeCompaction(backend.Host, backend.HttpPort, tabletId)

            def after = showTablet(tableName, backend.Host, backend.HttpPort, tabletId)
            def outputRowset = rowsetByVersion(after, 2)
            def outputInfo = parseRowsetInfo(outputRowset)
            assertEquals("NONOVERLAPPING_WITHIN_GROUP", outputInfo.overlap)
            def expectedOutputSegments =
                    (inputInfo.segments + inputSegmentsPerGroup - 1)
                            .intdiv(inputSegmentsPerGroup)
            if (expectMultipleOutputSegmentsPerGroup) {
                assertTrue(outputInfo.segments > expectedOutputSegments, outputRowset)
            } else {
                assertEquals(expectedOutputSegments, outputInfo.segments)
            }
            def countResult = sql "SELECT COUNT(*) FROM ${tableName}"
            assertEquals(expectedRows, countResult[0][0])
            def pointRowsAfterCompaction = checkPointRows(tableName, expectedPointRows)
            if (expectedPointRows == null && pointRowsAfterCompaction != pointRowsBeforeCompaction) {
                logger.warn("Point query result changed after single rowset grouped compaction" +
                        ", table=${tableName}, before=${pointRowsBeforeCompaction}" +
                        ", after=${pointRowsAfterCompaction}")
            }

            // Compact the grouped rowset as one range. This exercises VerticalBlockReader's
            // NONOVERLAPPING_WITHIN_GROUP iterator initialization and must produce a fully
            // non-overlapping rowset.
            set_be_param("cloud_single_rowset_compaction_segment_group_size",
                    outputInfo.segments.toString())
            runCumulativeCompaction(backend.Host, backend.HttpPort, tabletId)

            def afterSecondCompaction =
                    showTablet(tableName, backend.Host, backend.HttpPort, tabletId)
            def finalRowset = rowsetByVersion(afterSecondCompaction, 2)
            def finalInfo = parseRowsetInfo(finalRowset)
            assertEquals("NONOVERLAPPING", finalInfo.overlap)
            def finalCountResult = sql "SELECT COUNT(*) FROM ${tableName}"
            assertEquals(expectedRows, finalCountResult[0][0])
            def finalPointRows = checkPointRows(tableName, expectedPointRows)
            if (expectedPointRows == null) {
                assertEquals(pointRowsAfterCompaction, finalPointRows)
            }
        }

        GetDebugPoint().clearDebugPointsForAllBEs()

        try {
            GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")

            [2, 4].each { int inputSegmentsPerGroup ->
                checkSingleRowsetGroupedCompaction(
                        "test_cloud_single_rowset_grouped_compaction_g${inputSegmentsPerGroup}_dup",
                        "DUPLICATE KEY", "v INT", "", inputSegmentsPerGroup, 8192, 8192 * 2,
                        [["100", "100"], ["100", "101"]], false)
                checkSingleRowsetGroupedCompaction(
                        "test_cloud_single_rowset_grouped_compaction_g${inputSegmentsPerGroup}_agg",
                        "AGGREGATE KEY", "v INT SUM", "", inputSegmentsPerGroup, 8192, 8192,
                        [["100", "201"]], false)
                checkSingleRowsetGroupedCompaction(
                        "test_cloud_single_rowset_grouped_compaction_g${inputSegmentsPerGroup}_mow",
                        "UNIQUE KEY", "v INT",
                        ", \"enable_unique_key_merge_on_write\" = \"true\"",
                        inputSegmentsPerGroup, 8192, 8192, [["100", "101"]], false)
                checkSingleRowsetGroupedCompaction(
                        "test_cloud_single_rowset_grouped_compaction_g${inputSegmentsPerGroup}_mor",
                        "UNIQUE KEY", "v INT",
                        ", \"enable_unique_key_merge_on_write\" = \"false\"",
                        inputSegmentsPerGroup, 8192, 8192, null, false)
            }

            set_be_param("cloud_single_rowset_compaction_segment_group_size",
                    initialInputSegmentsPerGroup.toString())
            set_be_param("vertical_compaction_max_segment_size", "2048")
            set_be_param("compaction_batch_size", "512")
            checkSingleRowsetGroupedCompaction(
                    "test_cloud_single_rowset_grouped_compact_multi_seg_dup",
                    "DUPLICATE KEY", "v INT", "", initialInputSegmentsPerGroup, 32768, 32768 * 2,
                    [["100", "100"], ["100", "101"]], true)
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
