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

suite("test_mow_cumulative_compaction_multi_output_segments", "nonConcurrent") {
    if (!isCloudMode()) {
        logger.info("skip test_mow_cumulative_compaction_multi_output_segments in non-cloud mode")
        return
    }

    def tableName = "test_mow_cumulative_compaction_multi_output_segments"
    def backendIdToHost = [:]
    def backendIdToHttpPort = [:]
    getBackendIpHttpPort(backendIdToHost, backendIdToHttpPort)

    def configNames = [
            "compaction_batch_size",
            "doris_scanner_row_bytes",
            "enable_rowid_conversion_correctness_check",
            "enable_vertical_compaction",
            "vertical_compaction_max_segment_size"
    ]
    def originalConfigs = [:]

    def readBeConfig = { String backendId, String configName ->
        def host = backendIdToHost[backendId]
        def port = backendIdToHttpPort[backendId]
        def (code, out, err) = curl(
                "GET", "http://${host}:${port}/api/show_config?conf_item=${configName}")
        assertEquals(0, code)
        def configs = parseJson(out)
        assertEquals(1, configs.size())
        assertEquals(configName, configs[0][0])
        return configs[0][2].toString()
    }

    def updateBeConfig = { String configName, String value ->
        backendIdToHost.keySet().each { String backendId ->
            def host = backendIdToHost[backendId]
            def port = backendIdToHttpPort[backendId]
            def (code, out, err) = curl(
                    "POST", "http://${host}:${port}/api/update_config?${configName}=${value}")
            assertEquals(0, code)
            assertTrue(out.contains("OK"), "failed to set ${configName}=${value}: ${out}, ${err}")
        }
    }

    backendIdToHost.keySet().each { String backendId ->
        originalConfigs[backendId] = [:]
        configNames.each { String configName ->
            originalConfigs[backendId][configName] = readBeConfig(backendId, configName)
        }
    }

    def resetBeConfigs = {
        originalConfigs.each { String backendId, Map configs ->
            def host = backendIdToHost[backendId]
            def port = backendIdToHttpPort[backendId]
            configs.each { String configName, String value ->
                def (code, out, err) = curl(
                        "POST", "http://${host}:${port}/api/update_config?${configName}=${value}")
                assertEquals(0, code)
                assertTrue(out.contains("OK"),
                        "failed to reset ${configName}=${value}: ${out}, ${err}")
            }
        }
    }

    def showTablet = { def backend, String tabletId ->
        def (code, out, err) =
                be_show_tablet_status(backend.Host, backend.HttpPort, tabletId)
        logger.info("Show tablet status: code=${code}, out=${out}, err=${err}")
        assertEquals(0, code)
        return parseJson(out.trim())
    }

    def findRowset = { def tabletStatus, int startVersion, int endVersion ->
        def rowset =
                tabletStatus.rowsets.find { it.startsWith("[${startVersion}-${endVersion}] ") }
        assertNotNull(rowset,
                "cannot find rowset [${startVersion}-${endVersion}]: ${tabletStatus.rowsets}")
        return rowset
    }

    def parseRowset = { String rowset ->
        def matcher = rowset =~
                /\[[0-9]+-[0-9]+\]\s+([0-9]+)\s+DATA\s+([A-Z_]+)\s+([0-9a-f]+)/
        assertTrue(matcher.find(), "unexpected rowset format: ${rowset}")
        def segmentIds = []
        def segmentIdsMatcher = rowset =~ /\s\[([0-9]+(?:,[0-9]+)*)\]$/
        if (segmentIdsMatcher.find()) {
            segmentIds = segmentIdsMatcher.group(1).split(",").collect { it.toInteger() }
        }
        return [
                segmentNum: matcher.group(1).toInteger(),
                overlap: matcher.group(2),
                rowsetId: matcher.group(3),
                segmentIds: segmentIds
        ]
    }

    def waitForCompaction = { def backend, String tabletId ->
        long timeoutMs = 120000
        long deadline = System.currentTimeMillis() + timeoutMs
        def lastStatus = null
        while (System.currentTimeMillis() < deadline) {
            def (code, out, err) =
                    be_get_compaction_status(backend.Host, backend.HttpPort, tabletId)
            logger.info("Get compaction status: code=${code}, out=${out}, err=${err}")
            assertEquals(0, code)
            lastStatus = parseJson(out.trim())
            assertEquals("success", lastStatus.status.toLowerCase())
            if (!lastStatus.run_status) {
                return
            }
            Thread.sleep(1000)
        }
        assertTrue(false,
                "compaction timeout: tablet=${tabletId}, timeoutMs=${timeoutMs}, last=${lastStatus}")
    }

    def loadRows = { int startKey, int endKey, int valueBase ->
        StringBuilder content = new StringBuilder()
        (startKey..endKey).each { int key ->
            String suffix = key.toString().padLeft(32, "0")
            String payload = "payload_${suffix}_${(key * 17L).toString().padLeft(32, "0")}"
            content.append("${key},${valueBase + key},${payload}\n")
        }
        streamLoad {
            table tableName
            set "column_separator", ","
            inputStream new ByteArrayInputStream(content.toString().getBytes())
            time 120000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(endKey - startKey + 1, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }
        sql "sync"
    }

    def readRows = {
        return sql("""
            SELECT k, v
            FROM ${tableName}
            WHERE k IN (1, 16384, 16385, 32768, 32769, 49152, 49153, 65536, 65537, 81920)
            ORDER BY k
        """)
    }

    def getLocalDeleteBitmap = { def backend, String tabletId ->
        def (code, out, err) = curl(
                "GET",
                "http://${backend.Host}:${backend.HttpPort}" +
                        "/api/delete_bitmap/count_local?verbose=true&tablet_id=${tabletId}")
        logger.info("Get local delete bitmap: code=${code}, out=${out}, err=${err}")
        assertEquals(0, code)
        return parseJson(out.trim())
    }

    GetDebugPoint().clearDebugPointsForAllBEs()
    try {
        updateBeConfig("compaction_batch_size", "512")
        updateBeConfig("doris_scanner_row_bytes", "1")
        updateBeConfig("enable_rowid_conversion_correctness_check", "true")
        updateBeConfig("enable_vertical_compaction", "true")
        updateBeConfig("vertical_compaction_max_segment_size", "8192")

        GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
        GetDebugPoint().enableDebugPointForAllBEs(
                "VerticalBetaRowsetWriter.init.random_start_segment_id")

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                k INT NOT NULL,
                v BIGINT NOT NULL,
                payload VARCHAR(128) NOT NULL
            )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true"
            )
        """

        loadRows(1, 32768, 100000)
        loadRows(16385, 49152, 200000)
        loadRows(32769, 65536, 300000)
        // Keep this rowset outside the compaction range. Its updates create delete bitmap entries
        // on the [2-4] output rowset, exercising row-id conversion to physical segment ids.
        loadRows(49153, 81920, 400000)

        def countBefore = sql "SELECT COUNT(*) FROM ${tableName}"
        assertEquals(81920, countBefore[0][0])
        def rowsBefore = readRows()

        def tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
        assertEquals(1, tablets.size())
        def tabletId = tablets[0].TabletId.toString()
        def backendId = tablets[0].BackendId.toString()
        def backends = sql_return_maparray "SHOW BACKENDS"
        def backend = backends.find { it.BackendId.toString() == backendId }
        assertNotNull(backend)

        def before = showTablet(backend, tabletId)
        [2, 3, 4].each { int version ->
            def inputRowset = findRowset(before, version, version)
            def inputInfo = parseRowset(inputRowset)
            assertTrue(inputInfo.segmentNum > 1, inputRowset)
        }
        def untouchedRowset = findRowset(before, 5, 5)

        GetDebugPoint().enableDebugPointForAllBEs(
                "CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
                [tablet_id: tabletId, start_version: "2", end_version: "4"])
        def (code, out, err) =
                be_run_cumulative_compaction(backend.Host, backend.HttpPort, tabletId)
        logger.info("Run compaction: code=${code}, out=${out}, err=${err}")
        assertEquals(0, code)
        def compactResult = parseJson(out.trim())
        assertEquals("success", compactResult.status.toLowerCase())
        waitForCompaction(backend, tabletId)

        def after = showTablet(backend, tabletId)
        def outputRowset = findRowset(after, 2, 4)
        def outputInfo = parseRowset(outputRowset)
        assertEquals("NONOVERLAPPING", outputInfo.overlap)
        assertTrue(outputInfo.segmentNum > 1, outputRowset)
        assertEquals(outputInfo.segmentNum, outputInfo.segmentIds.size())
        assertTrue(outputInfo.segmentIds[0] > 0, outputRowset)
        for (int i = 1; i < outputInfo.segmentIds.size(); ++i) {
            assertEquals(outputInfo.segmentIds[i - 1] + 1, outputInfo.segmentIds[i])
        }
        assertEquals(untouchedRowset, findRowset(after, 5, 5))

        def countAfter = sql "SELECT COUNT(*) FROM ${tableName}"
        assertEquals(countBefore, countAfter)
        assertEquals(rowsBefore, readRows())

        def deleteBitmap = getLocalDeleteBitmap(backend, tabletId)
        assertNotNull(deleteBitmap.delete_bitmap)
        def outputDeleteBitmapKeys = deleteBitmap.delete_bitmap.keySet().findAll {
            it.contains("rowset: ${outputInfo.rowsetId},")
        }
        assertFalse(outputDeleteBitmapKeys.isEmpty(),
                "missing delete bitmap for output rowset ${outputInfo.rowsetId}: ${deleteBitmap}")
        outputDeleteBitmapKeys.each { String key ->
            def matcher = key =~ /segment:\s+([0-9]+)/
            assertTrue(matcher.find(), "unexpected delete bitmap key: ${key}")
            assertTrue(outputInfo.segmentIds.contains(matcher.group(1).toInteger()),
                    "delete bitmap references a segment outside ${outputInfo.segmentIds}: ${key}")
        }
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
        resetBeConfigs()
    }
}
