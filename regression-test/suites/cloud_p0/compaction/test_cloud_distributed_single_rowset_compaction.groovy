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

suite("test_cloud_distributed_single_rowset_compaction", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(2)
    options.enableDebugPoints()
    options.beConfigs += [
        "doris_scanner_row_bytes=1",
        "enable_cloud_single_rowset_compaction=true",
        "cloud_single_rowset_compaction_min_segments=2",
        "cloud_single_rowset_compaction_segment_group_size=2",
        "cloud_distributed_compaction_status_poll_interval_ms=100",
        "enable_cloud_single_rowset_distributed_compaction=true",
        "cumulative_compaction_min_deltas=2",
        "disable_auto_compaction=true",
        "enable_aggregate_non_mow_key_bounds=false",
        "enable_java_support=false"
    ]

    docker(options) {
        long compactionTimeoutMs = 90000L

        def showTablet = { String beHost, String bePort, String tabletId ->
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

        def waitForCompactionState = {
                String beHost, String bePort, String tabletId, boolean running ->
            long deadline = System.currentTimeMillis() + compactionTimeoutMs
            def lastStatus = null
            while (System.currentTimeMillis() < deadline) {
                def (code, out, err) = be_get_compaction_status(beHost, bePort, tabletId)
                logger.info("Get compaction status: code=${code}, out=${out}, err=${err}")
                assertEquals(0, code)
                lastStatus = parseJson(out.trim())
                assertEquals("success", lastStatus.status.toLowerCase())
                if (lastStatus.run_status == running) {
                    return
                }
                Thread.sleep(100)
            }
            assertTrue(false, "compaction did not reach running=${running} on " +
                    "${beHost}:${bePort}, tablet=${tabletId}, last=${lastStatus}")
        }

        def runCumulativeCompaction = { String beHost, String bePort, String tabletId ->
            def (code, out, err) =
                    be_run_cumulative_compaction(beHost, bePort, tabletId)
            logger.info("Run compaction: code=${code}, out=${out}, err=${err}")
            assertEquals(0, code)
            assertEquals("success", parseJson(out.trim()).status.toLowerCase())
            waitForCompactionState(beHost, bePort, tabletId, false)
        }

        def latestCumulativeProfile = { String beHost, String bePort, String tabletId ->
            def profileUrl = "http://${beHost}:${bePort}/api/compaction/profile" +
                    "?tablet_id=${tabletId}&compact_type=cumulative&top_n=1"
            def (code, out, err) = curl("GET", profileUrl)
            logger.info("Get compaction profile: code=${code}, out=${out}, err=${err}")
            assertEquals(0, code)
            def response = parseJson(out.trim())
            assertEquals("Success", response.status)
            assertEquals(1, response.compaction_profiles.size())
            def profile = response.compaction_profiles[0]
            assertTrue(profile.success)
            return profile
        }

        def backends = sql_return_maparray "SHOW BACKENDS"
        assertEquals(2, backends.size())
        logger.info("Distributed single-rowset workers include the coordinator discovered from FE")

        def checkDistributedSingleRowsetCompaction = {
                String model, String keyType, String valueColumn, String indexDefinition,
                String extraProperties, String indexPredicate, boolean hasSequenceColumn ->
            String tableName =
                    "test_cloud_distributed_single_rowset_compaction_${model}"
            sql "DROP TABLE IF EXISTS ${tableName}"
            sql """
                CREATE TABLE ${tableName} (
                    k INT,
                    ${valueColumn}${indexDefinition}
                )
                ${keyType}(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true"${extraProperties}
                )
            """

            StringBuilder content = new StringBuilder()
            for (int round = 0; round < 2; ++round) {
                (1..8192).each {
                    if (hasSequenceColumn) {
                        content.append("${it},${it + round},${round}\n")
                    } else {
                        content.append("${it},${it + round}\n")
                    }
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
                    assertEquals(16384, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            sql "sync"

            String pointQuery = """
                SELECT k, v
                FROM ${tableName}
                WHERE k IN (1, 4096, 8192)
                ORDER BY k, v
            """
            def rowsBeforeCompaction = sql pointQuery
            "order_qt_${model}_rows_before" pointQuery
            def countBeforeCompaction = sql "SELECT COUNT(*) FROM ${tableName}"
            "qt_${model}_count_before" "SELECT COUNT(*) FROM ${tableName}"

            def indexRowsBeforeCompaction = null
            String indexQuery = null
            if (indexPredicate != null) {
                indexQuery = """
                    SELECT k, v
                    FROM ${tableName}
                    WHERE ${indexPredicate}
                    ORDER BY k, v
                """
                indexRowsBeforeCompaction = sql indexQuery
                "order_qt_${model}_index_rows_before" indexQuery
            }

            def tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
            assertEquals(1, tablets.size())
            def tabletId = tablets[0].TabletId
            def coordinatorBackendId = tablets[0].BackendId
            def coordinator = backends.find { it.BackendId == coordinatorBackendId }
            assertNotNull(coordinator)

            def before = showTablet(coordinator.Host, coordinator.HttpPort, tabletId)
            def inputRowset = rowsetByVersion(before, 2)
            def inputInfo = parseRowsetInfo(inputRowset)
            assertEquals("OVERLAPPING", inputInfo.overlap)
            assertTrue(inputInfo.segments >= 6, inputRowset)

            runCumulativeCompaction(coordinator.Host, coordinator.HttpPort, tabletId)

            def after = showTablet(coordinator.Host, coordinator.HttpPort, tabletId)
            def outputRowset = rowsetByVersion(after, 2)
            def outputInfo = parseRowsetInfo(outputRowset)
            assertEquals("NONOVERLAPPING_WITHIN_GROUP", outputInfo.overlap)
            assertEquals((inputInfo.segments + 1).intdiv(2), outputInfo.segments)

            def rowsAfterCompaction = sql pointQuery
            assertEquals(rowsBeforeCompaction, rowsAfterCompaction)
            "order_qt_${model}_rows_after" pointQuery
            def countAfterCompaction = sql "SELECT COUNT(*) FROM ${tableName}"
            assertEquals(countBeforeCompaction, countAfterCompaction)
            "qt_${model}_count_after" "SELECT COUNT(*) FROM ${tableName}"
            if (indexPredicate != null) {
                def indexRowsAfterCompaction = sql indexQuery
                assertEquals(indexRowsBeforeCompaction, indexRowsAfterCompaction)
                "order_qt_${model}_index_rows_after" indexQuery
            }

            def profile = latestCumulativeProfile(
                    coordinator.Host, coordinator.HttpPort, tabletId)
            assertTrue(profile.is_distributed)
            assertEquals(outputInfo.segments, profile.distributed_task_count.toString().toInteger())
            assertEquals(backends.size(), profile.distributed_worker_count.toString().toInteger())

            def backendsAfterCompaction = sql_return_maparray "SHOW BACKENDS"
            assertEquals(2, backendsAfterCompaction.size())
            backendsAfterCompaction.each {
                assertEquals("true", it.Alive.toString().toLowerCase())
            }

            if (model == "mow") {
                String beforeIncrementalLockDebugPoint =
                        "CloudCumulativeCompaction::" +
                        "finish_distributed_mow_delete_bitmap.before_lock"
                GetDebugPoint().enableDebugPointForAllBEs(beforeIncrementalLockDebugPoint)
                def (code, out, err) =
                        be_run_cumulative_compaction(coordinator.Host, coordinator.HttpPort, tabletId)
                logger.info("Run incremental compaction: code=${code}, out=${out}, err=${err}")
                assertEquals(0, code)
                assertEquals("success", parseJson(out.trim()).status.toLowerCase())
                waitForCompactionState(coordinator.Host, coordinator.HttpPort, tabletId, true)
                // Allow phase 1 to snapshot the tablet before publishing a new version.
                Thread.sleep(1000)

                sql "INSERT INTO ${tableName} VALUES (42, 424242, 2)"
                sql "sync"
                def rowsAfterIncrementalLoad = sql "SELECT v FROM ${tableName} WHERE k = 42"
                GetDebugPoint().disableDebugPointForAllBEs(beforeIncrementalLockDebugPoint)
                waitForCompactionState(coordinator.Host, coordinator.HttpPort, tabletId, false)

                def incrementalProfile = latestCumulativeProfile(
                        coordinator.Host, coordinator.HttpPort, tabletId)
                assertTrue(incrementalProfile.compaction_id > profile.compaction_id)
                assertTrue(incrementalProfile.is_distributed)
                assertEquals((outputInfo.segments + 1).intdiv(2),
                        incrementalProfile.distributed_task_count.toString().toInteger())
                assertEquals(backends.size(),
                        incrementalProfile.distributed_worker_count.toString().toInteger())
                assertEquals(rowsAfterIncrementalLoad,
                        sql("SELECT v FROM ${tableName} WHERE k = 42"))
            }
        }

        GetDebugPoint().clearDebugPointsForAllBEs()
        try {
            GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")

            checkDistributedSingleRowsetCompaction(
                    "dup", "DUPLICATE KEY", "v INT", "", "", null, false)
            checkDistributedSingleRowsetCompaction(
                    "agg", "AGGREGATE KEY", "v INT SUM", "", "", null, false)
            checkDistributedSingleRowsetCompaction(
                    "mow", "UNIQUE KEY", "v INT, seq BIGINT", "",
                    ", \"enable_unique_key_merge_on_write\" = \"true\"" +
                            ", \"function_column.sequence_col\" = \"seq\"",
                    null, true)
            checkDistributedSingleRowsetCompaction(
                    "mor", "UNIQUE KEY", "v INT, seq BIGINT", "",
                    ", \"enable_unique_key_merge_on_write\" = \"false\"" +
                            ", \"function_column.sequence_col\" = \"seq\"",
                    null, true)
            checkDistributedSingleRowsetCompaction(
                    "inverted_index", "DUPLICATE KEY", "v STRING",
                    ", INDEX idx_v(v) USING INVERTED PROPERTIES(\"parser\" = \"english\")",
                    "", "v MATCH_ALL '4096'", false)
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
