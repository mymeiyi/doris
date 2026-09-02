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

suite("test_cloud_distributed_base_compaction", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(2)
    options.feConfigs += [
        "tablet_stat_update_interval_second=1",
        "enable_date_conversion=false",
        "disable_datev1=false",
        "disable_decimalv2=false"
    ]
    options.beConfigs += [
        "enable_cloud_distributed_base_compaction=true",
        "cloud_distributed_compaction_status_poll_interval_ms=100",
        "base_compaction_min_rowset_num=2",
        "cumulative_compaction_min_deltas=2",
        "compaction_promotion_min_size_mbytes=0",
        "disable_auto_compaction=true",
        "enable_aggregate_non_mow_key_bounds=false",
        "enable_java_support=false"
    ]

    docker(options) {
        sql "set enable_decimal256 = true"

        long compactionTimeoutMs = 90000L

        def showTablet = { String beHost, String bePort, String tabletId ->
            def (code, out, err) = be_show_tablet_status(beHost, bePort, tabletId)
            logger.info("Show tablet status: code=${code}, out=${out}, err=${err}")
            assertEquals(0, code)
            return parseJson(out.trim())
        }

        def keyCases = [
            [name: "tinyint", type: "TINYINT", keyExpr: "CAST(number % 128 - 64 AS TINYINT)"],
            [name: "smallint", type: "SMALLINT", keyExpr: "CAST(number - 4096 AS SMALLINT)"],
            [name: "int", type: "INT", keyExpr: "CAST(number * 100000 - 409600000 AS INT)"],
            [name: "bigint", type: "BIGINT",
             keyExpr: "CAST(number * 1000000000000 - 4096000000000000 AS BIGINT)"],
            [name: "largeint", type: "LARGEINT", keyExpr: """
                CAST(number AS LARGEINT) * CAST('1000000000000000000000000' AS LARGEINT)
                    - CAST('4096000000000000000000000000' AS LARGEINT)
            """],
            [name: "datev2", type: "DATEV2", keyExpr: """
                CAST(DAYS_ADD(CAST('2000-01-01' AS DATEV2), CAST(number AS INT)) AS DATEV2)
            """],
            [name: "datetimev2", type: "DATETIMEV2(6)", keyExpr: """
                CAST(SECONDS_ADD(CAST('2000-01-01 00:00:00.123456' AS DATETIMEV2(6)),
                    CAST(number AS INT)) AS DATETIMEV2(6))
            """],
            [name: "decimalv2", type: "DECIMALV2(27, 9)",
             keyExpr: "CAST(number - 4096 AS DECIMALV2(27, 9))"],
            [name: "decimal32", type: "DECIMALV3(9, 2)",
             keyExpr: "CAST(number - 4096 AS DECIMALV3(9, 2))"],
            [name: "decimal64", type: "DECIMALV3(18, 2)",
             keyExpr: "CAST(number - 4096 AS DECIMALV3(18, 2))"],
            [name: "decimal128i", type: "DECIMALV3(38, 2)",
             keyExpr: "CAST(number - 4096 AS DECIMALV3(38, 2))"],
            [name: "decimal256", type: "DECIMALV3(76, 2)",
             keyExpr: "CAST(number - 4096 AS DECIMALV3(76, 2))"],
            [name: "char", type: "CHAR(64)", keyExpr: """
                CAST(CONCAT(LPAD(CAST(number AS STRING), 5, '0'), REPEAT('c', 59)) AS CHAR(64))
            """],
            [name: "varchar", type: "VARCHAR(128)", keyExpr: """
                CONCAT(REPEAT('v', 123), LPAD(CAST(number AS STRING), 5, '0'))
            """],
            [name: "composite", keyColumns: "k INT NOT NULL, k2 VARCHAR(128) NOT NULL",
             keyExpr: "CAST(0 AS INT), CONCAT('key-', LPAD(CAST(number AS STRING), 5, '0'))",
             keyModelColumns: "k, k2", sampleKey: "k, k2"],
            [name: "nullable_composite", keyColumns: "k INT NULL, k2 INT NOT NULL",
             keyExpr: "CAST(NULL AS INT), CAST(number AS INT)",
             keyModelColumns: "k, k2", sampleKey: "k, k2"],
            [name: "agg", type: "INT", keyExpr: "CAST(number AS INT)",
             keyModel: "AGGREGATE KEY", valueColumn: "v BIGINT SUM"],
            [name: "mor", type: "INT", keyExpr: "CAST(number AS INT)",
             keyModel: "UNIQUE KEY", valueColumn: "v INT NOT NULL",
             properties: ', "enable_unique_key_merge_on_write" = "false"'],
            [name: "mow", keyColumns: "k INT NOT NULL, k2 BIGINT NOT NULL",
             keyExpr: "CAST(0 AS INT), CAST(number AS BIGINT)", keyModelColumns: "k, k2",
             sampleKey: "k, k2",
             keyModel: "UNIQUE KEY", valueColumn: "v INT NOT NULL, seq BIGINT NOT NULL",
             valueExpr: "CAST(number + ROUND * 10000 AS INT), " +
                     "CAST(number + ROUND * 10000 AS BIGINT)",
             properties: ', "enable_unique_key_merge_on_write" = "true"' +
                     ', "function_column.sequence_col" = "seq"']
        ]

        keyCases.each { keyCase ->
            String tableName = "test_cloud_distributed_base_compaction_${keyCase.name}"
            String keyModel = keyCase.keyModel ?: "DUPLICATE KEY"
            String keyColumns = keyCase.keyColumns ?: "k ${keyCase.type} NOT NULL"
            String keyModelColumns = keyCase.keyModelColumns ?: "k"
            String sampleKey = keyCase.sampleKey ?: "k"
            String valueColumn = keyCase.valueColumn ?: "v INT NOT NULL"
            String valueExpr = keyCase.valueExpr ?: "CAST(number + ROUND * 10000 AS INT)"
            String extraProperties = keyCase.properties ?: ""
            sql "DROP TABLE IF EXISTS ${tableName}"
            sql """
                CREATE TABLE ${tableName} (
                    ${keyColumns},
                    ${valueColumn}
                ) ${keyModel}(${keyModelColumns})
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true"${extraProperties}
                )
            """

            for (int round = 0; round < 6; ++round) {
                sql """
                    INSERT INTO ${tableName}
                    SELECT ${keyCase.keyExpr}, ${valueExpr.replace("ROUND", round.toString())}
                    FROM numbers("number" = "8192")
                """
                if (round % 2 == 1) {
                    sql "sync"
                    trigger_and_wait_compaction(tableName, "cumulative")
                }
            }

            def summaryBefore = sql """
                SELECT COUNT(*), SUM(v), MIN(k), MAX(k)
                FROM ${tableName}
            """
            def lowSamplesBefore = sql """
                SELECT ${sampleKey}, COUNT(*), SUM(v)
                FROM ${tableName}
                GROUP BY ${sampleKey}
                ORDER BY ${sampleKey}
                LIMIT 4
            """
            def highSamplesBefore = sql """
                SELECT ${sampleKey}, COUNT(*), SUM(v)
                FROM ${tableName}
                GROUP BY ${sampleKey}
                ORDER BY ${sampleKey} DESC
                LIMIT 4
            """

            def backends = sql_return_maparray "SHOW BACKENDS"
            assertEquals(2, backends.size())
            sql "set cloud_force_sync_tablet_stats = true"
            def tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
            assertEquals(1, tablets.size())
            String tabletId = tablets[0].TabletId
            String coordinatorBackendId = tablets[0].BackendId
            def coordinator = backends.find {
                it.BackendId.toString() == coordinatorBackendId.toString()
            }
            assertNotNull(coordinator)

            Thread.sleep(2000)
            long inputSizeBytes = 0
            long reportDeadline = System.currentTimeMillis() + compactionTimeoutMs
            while (System.currentTimeMillis() < reportDeadline) {
                tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
                assertEquals(1, tablets.size())
                inputSizeBytes = tablets[0].RemoteDataSize.toString().toLong()
                if (inputSizeBytes > 0) {
                    break
                }
                Thread.sleep(100)
            }
            sql "set cloud_force_sync_tablet_stats = false"
            assertTrue(inputSizeBytes > 0)

            int expectedTaskCount = 2 * backends.size()
            long targetInputSizeBytes =
                    (inputSizeBytes + expectedTaskCount - 1) / expectedTaskCount
            backends.each { backend ->
                def (code, out, err) = update_be_config(
                        backend.Host, backend.HttpPort,
                        "cloud_distributed_base_compaction_target_range_input_size_bytes",
                        targetInputSizeBytes.toString())
                logger.info("Set distributed Base target on BE ${backend.BackendId}: " +
                        "code=${code}, out=${out}, err=${err}, target=${targetInputSizeBytes}")
                assertEquals(0, code)
                assertTrue(out.contains("OK"))
            }

            def before = showTablet(coordinator.Host, coordinator.HttpPort, tabletId)
            int inputRowsetCount = before.rowsets.count { it.contains(" DATA ") }
            assertTrue(inputRowsetCount >= 4, "expected at least four input rowsets: ${before.rowsets}")

            def (code, out, err) =
                    be_run_base_compaction(coordinator.Host, coordinator.HttpPort, tabletId)
            logger.info("Run base compaction: code=${code}, out=${out}, err=${err}")
            assertEquals(0, code)
            assertEquals("success", parseJson(out.trim()).status.toLowerCase())

            def after = null
            long deadline = System.currentTimeMillis() + compactionTimeoutMs
            while (System.currentTimeMillis() < deadline) {
                after = showTablet(coordinator.Host, coordinator.HttpPort, tabletId)
                if (after.rowsets.count { it.contains(" DATA ") } < inputRowsetCount &&
                        after["last base status"] == "[OK]") {
                    break
                }
                Thread.sleep(1000)
            }
            assertNotNull(after)
            assertEquals("[OK]", after["last base status"])
            def outputRowsets = after.rowsets.findAll { it =~ /\]\s+${expectedTaskCount}\s+DATA\s+/ }
            assertEquals(1, outputRowsets.size())
            def outputMatcher =
                    outputRowsets[0] =~ /\[[0-9]+-[0-9]+\]\s+([0-9]+)\s+DATA\s+([A-Z_]+)/
            assertTrue(outputMatcher.find(), "unexpected output rowset: ${outputRowsets[0]}")
            assertEquals(expectedTaskCount, outputMatcher.group(1).toInteger())
            assertEquals("NONOVERLAPPING", outputMatcher.group(2))

            def profileUrl = "http://${coordinator.Host}:${coordinator.HttpPort}" +
                    "/api/compaction/profile?tablet_id=${tabletId}" +
                    "&compact_type=base&success=true&top_n=1"
            def (profileCode, profileOut, profileErr) = curl("GET", profileUrl)
            logger.info("Get compaction profile: code=${profileCode}, out=${profileOut}, " +
                    "err=${profileErr}")
            assertEquals(0, profileCode)
            def profileResponse = parseJson(profileOut.trim())
            assertEquals("Success", profileResponse.status)
            def profiles = profileResponse.compaction_profiles
            assertEquals(1, profiles.size())
            def profile = profiles[0]
            assertTrue(profile.is_distributed)
            assertEquals(expectedTaskCount, profile.distributed_task_count.toString().toInteger())
            assertEquals(backends.size(),
                    profile.distributed_worker_count.toString().toInteger())

            assertEquals(summaryBefore, sql("""
                SELECT COUNT(*), SUM(v), MIN(k), MAX(k)
                FROM ${tableName}
            """))
            assertEquals(lowSamplesBefore, sql("""
                SELECT ${sampleKey}, COUNT(*), SUM(v)
                FROM ${tableName}
                GROUP BY ${sampleKey}
                ORDER BY ${sampleKey}
                LIMIT 4
            """))
            assertEquals(highSamplesBefore, sql("""
                SELECT ${sampleKey}, COUNT(*), SUM(v)
                FROM ${tableName}
                GROUP BY ${sampleKey}
                ORDER BY ${sampleKey} DESC
                LIMIT 4
            """))

            def backendsAfterCompaction = sql_return_maparray "SHOW BACKENDS"
            assertEquals(2, backendsAfterCompaction.size())
            backendsAfterCompaction.each {
                assertEquals("true", it.Alive.toString().toLowerCase())
            }
        }
    }
}
