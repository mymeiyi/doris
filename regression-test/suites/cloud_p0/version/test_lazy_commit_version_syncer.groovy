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

import groovy.json.JsonSlurper
import org.apache.doris.regression.suite.ClusterOptions

import java.util.concurrent.TimeUnit

suite("test_lazy_commit_version_syncer", "docker") {
    if (!isCloudMode()) {
        return
    }

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true
    options.feConfigs += [
        'enable_cloud_txn_lazy_commit=true',
        'cloud_enable_version_syncer=true',
        'cloud_version_syncer_interval_second=1',
        'meta_service_brpc_timeout_ms=30000',
        'sys_log_verbose_modules=org.apache.doris.cloud'
    ]
    options.msConfigs += [
        'enable_cloud_txn_lazy_commit=true',
        'txn_lazy_commit_rowsets_thresold=0',
        // Keep BE background reads from completing the pending transaction before the daemon sees it.
        'advance_txn_lazy_commit_during_reads=false'
    ]

    docker(options) {
        def ms = cluster.getAllMetaservices().get(0)
        // Requires an MS binary built with ENABLE_INJECTION_POINT, as do other MS injection suites.
        def injectMs = { String parameters ->
            httpTest {
                op "get"
                endpoint "${ms.host}:${ms.httpPort}"
                uri "/MetaService/http/v1/injection_point?token=greedisgood9999&${parameters}"
                check { responseCode, body ->
                    assertEquals(200, responseCode)
                    assertEquals("OK", new JsonSlurper().parseText(body).code,
                            "MS injection request failed: ${parameters}; ${body}")
                }
            }
        }

        sql "DROP TABLE IF EXISTS test_lazy_commit_version_syncer FORCE"
        sql """
            CREATE TABLE test_lazy_commit_version_syncer (k INT NOT NULL, v INT NOT NULL)
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
        """
        // Initial [0-1] plus eleven inserts gives visible version 12. DELETE will publish version 13.
        for (int i = 1; i <= 11; ++i) {
            sql "INSERT INTO test_lazy_commit_version_syncer VALUES (${i}, ${i})"
        }
        // Observe FE's cache throughout the case; SHOW PARTITIONS must not repair it via an expired TTL.
        sql "SET cloud_force_sync_version = false"
        sql "SET cloud_partition_version_cache_ttl_ms = 3600000"
        sql "SET cloud_table_version_cache_ttl_ms = 3600000"
        def cachedVersion = {
            def partitions = sql_return_maparray "SHOW PARTITIONS FROM test_lazy_commit_version_syncer"
            assertEquals(1, partitions.size())
            return partitions[0].VisibleVersion.toLong()
        }
        assertEquals(12L, cachedVersion())

        def feLog = new File(cluster.getMasterFe().getLogFilePath())
        def msLog = new File(ms.getLogFilePath())
        int feLogStart = feLog.getText("UTF-8").length()
        int msLogStart = msLog.getText("UTF-8").length()
        def feLogSinceDelete = { feLog.getText("UTF-8").substring(feLogStart) }
        def msLogSinceDelete = { msLog.getText("UTF-8").substring(msLogStart) }
        def phaseOnePoint = "commit_txn_eventually::abort_txn_after_mark_txn_commited"
        // This point is after task submission: the worker can finish while the original RPC sleeps.
        // Unlike task->wait, its name contains no characters requiring URL encoding.
        def responsePoint = "commit_txn_eventually::txn_lazy_committer_wait"
        def phaseOneHit = "injection point hit, point=${phaseOnePoint} sleep ms=15000"
        def responseHit = "injection point hit, point=${responsePoint} sleep ms=40000"
        def deleteFuture = null
        try {
            // Both points are outside an uncommitted FDB transaction. Sleeping inside the second-phase
            // KV commit would instead expire that transaction and reproduce an unrelated failure.
            // MS reads name through brpc::URI::GetQuery without URL decoding. Keep the literal '::';
            // encoding it registers a different callback name, even though the API returns OK.
            injectMs("op=set&name=${phaseOnePoint}&behavior=sleep&duration=15000")
            injectMs("op=set&name=${responsePoint}&behavior=sleep&duration=40000")
            injectMs("op=enable")

            deleteFuture = thread("lazy-commit-delete") {
                sql "DELETE FROM test_lazy_commit_version_syncer WHERE k = 1"
            }
            awaitUntil(10, 0.1) { msLogSinceDelete().contains(phaseOneHit) }
            // Verify the intended overlap instead of assuming the daemon ran during the 15-second pause.
            // A fixed daemon may wait for/complete the pending transaction here, which is also valid.
            awaitUntil(10, 0.1) {
                feLogSinceDelete().readLines().any {
                    it.contains("sync partition version for db:") &&
                            it.contains("name=test_lazy_commit_version_syncer,")
                }
            }
            assertFalse(msLogSinceDelete().contains(responseHit),
                    "The daemon must start synchronizing before the original commit leaves the phase-one pause")

            deleteFuture.get(60, TimeUnit.SECONDS)
            assertTrue(msLogSinceDelete().contains(responseHit), "The first successful response must be delayed")
            assertTrue(feLogSinceDelete().contains("failed to request meta service code DEADLINE_EXCEEDED"),
                    "The original commit RPC must time out")
            assertTrue(msLogSinceDelete().contains("transaction is already visible: db_id="),
                    "The real MS retry must take the VISIBLE branch without partition versions/table stats")

            // Give subsequent daemon cycles a chance to recover. Their own one-second cache interval
            // is independent of the long session TTL used by SHOW PARTITIONS above.
            int afterCommit = feLogSinceDelete().length()
            awaitUntil(10, 0.2) {
                feLogSinceDelete().substring(afterCommit).readLines().count {
                    it.contains("sync table version cost")
                } >= 2
            }
            // Intentionally fails on the unfixed code with actual version 12. This checks the cause of
            // E-230 before BE compaction makes querying that obsolete version impossible.
            assertEquals(13L, cachedVersion(),
                    "DELETE is visible in MS, but daemon table-version equality leaves FE partition version at 12")
        } finally {
            injectMs("op=disable")
            injectMs("op=clear&name=${phaseOnePoint}")
            injectMs("op=clear&name=${responsePoint}")
            if (deleteFuture != null && !deleteFuture.isDone()) {
                deleteFuture.get(60, TimeUnit.SECONDS)
            }
        }
    }
}
