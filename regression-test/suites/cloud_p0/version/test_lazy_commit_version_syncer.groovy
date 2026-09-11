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
        'meta_service_brpc_timeout_ms=30000'
    ]
    options.msConfigs += [
        'enable_cloud_txn_lazy_commit=true',
        'txn_lazy_commit_rowsets_thresold=0',
        // Keep BE background reads from completing the pending transaction before the daemon sees it.
        'advance_txn_lazy_commit_during_reads=false'
    ]

    docker(options) {
        def ms = cluster.getAllMetaservices().get(0)
        def fe = cluster.getMasterFe()
        // These counters are process-wide. Use an isolated cluster with one table and one in-flight DELETE.
        def feRpcCount = { String counter, String method ->
            def metrics
            httpTest {
                op "get"
                endpoint "${fe.host}:${fe.httpPort}"
                uri "/metrics?type=json"
                check { code, body ->
                    assertEquals(200, code)
                    metrics = new JsonSlurper().parseText(body)
                }
            }
            def entry = metrics.find {
                it.tags.metric == "doris_fe_meta_service_rpc_${counter}" && it.tags.method == method
            }
            // Per-method metrics are created on the first RPC/retry, so an absent counter is initially zero.
            return entry == null ? 0L : entry.value.toLong()
        }
        def lazyCommitCount = { String counter ->
            def name = "txn_lazy_committer_${counter}"
            def (code, out, err) = curl("GET", "http://${ms.host}:${ms.httpPort}/vars/${name}")
            assertEquals(0, code)
            def matcher = out =~ /(?m)^${name}\s*:\s*(\d+)$/
            assertTrue(matcher.find(), "Missing MS counter ${name}: ${out}; ${err}")
            return matcher.group(1).toLong()
        }
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
        // The daemon runs only for never-expiring global caches on branches with the TTL guard.
        sql "SET GLOBAL cloud_partition_version_cache_ttl_ms = 9223372036854775807"
        sql "SET GLOBAL cloud_table_version_cache_ttl_ms = 9223372036854775807"
        sql "SET cloud_partition_version_cache_ttl_ms = 9223372036854775807"
        sql "SET cloud_table_version_cache_ttl_ms = 9223372036854775807"
        def cachedVersion = {
            def partitions = sql_return_maparray "SHOW PARTITIONS FROM test_lazy_commit_version_syncer"
            assertEquals(1, partitions.size())
            return partitions[0].VisibleVersion.toLong()
        }
        assertEquals(12L, cachedVersion())

        awaitUntil(10, 0.1) { lazyCommitCount("submitted") == lazyCommitCount("finished") }
        long lazyBefore = lazyCommitCount("submitted")
        long partitionRequestsBefore = feRpcCount("total", "getPartitionVersion")
        long commitRetriesBefore = feRpcCount("retry", "commitTxn")
        def phaseOnePoint = "commit_txn_eventually::abort_txn_after_mark_txn_commited"
        // This point is after task submission: the worker can finish while the original RPC sleeps.
        // Unlike task->wait, its name contains no characters requiring URL encoding.
        def responsePoint = "commit_txn_eventually::txn_lazy_committer_wait"
        def deleteFuture = null
        String stage = "register MS injection points"
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
            stage = "wait for daemon partition version RPC"
            awaitUntil(10, 0.1) {
                if (deleteFuture.isDone()) {
                    deleteFuture.get()
                    throw new IllegalStateException("DELETE completed before the injected pause was observed. " +
                            "Check MS ENABLE_INJECTION_POINT=ON and lazy commit configuration; " +
                            "HTTP OK is insufficient.")
                }
                return feRpcCount("total", "getPartitionVersion") > partitionRequestsBefore
            }
            // A total counter advances at RPC entry. Waiting for the next daemon round ensures that the
            // preceding partition RPC and cache update have completed, without reading asynchronous logs.
            stage = "wait for completion of the first daemon sync"
            long tableRequestsDuringSync = feRpcCount("total", "getTableVersion")
            awaitUntil(5, 0.1) { feRpcCount("total", "getTableVersion") > tableRequestsDuringSync }
            // On the unfixed daemon, the lazy task has not been submitted during the phase-one pause.
            // A daemon using waitForPendingTxns=true may submit/finish it early and must then cache 13.
            assertTrue(lazyCommitCount("submitted") == lazyBefore || cachedVersion() == 13L,
                    "The first daemon sync must finish before lazy submission, or recover partition version 13")

            stage = "wait for the lazy task to finish before the original RPC returns"
            awaitUntil(20, 0.1) { lazyCommitCount("finished") == lazyBefore + 1 }
            assertFalse(deleteFuture.isDone(), "The original commit response must still be delayed")
            assertEquals(commitRetriesBefore, feRpcCount("retry", "commitTxn"),
                    "Lazy commit must finish before the first commit RPC retry")

            stage = "wait for DELETE retry after lazy commit"
            deleteFuture.get(60, TimeUnit.SECONDS)
            assertTrue(feRpcCount("retry", "commitTxn") > commitRetriesBefore,
                    "The delayed response must cause a real commit RPC retry")
            assertEquals(lazyBefore + 1, lazyCommitCount("submitted"),
                    "The successful retry must not submit a second lazy task")
            // The only lazy task finished before any retry, and DELETE succeeded without another task:
            // the retry observes VISIBLE. No MS metadata keys or transaction log text are inspected.

            stage = "wait for two daemon cycles after DELETE"
            long tableRequestsAfterCommit = feRpcCount("total", "getTableVersion")
            awaitUntil(10, 0.2) {
                // Entry into the third round guarantees the first two rounds have finished.
                feRpcCount("total", "getTableVersion") >= tableRequestsAfterCommit + 3
            }
            // Intentionally fails on the unfixed code with actual version 12. This checks the cause of
            // E-230 before BE compaction makes querying that obsolete version impossible.
            stage = "verify FE cached partition version is 13"
            assertEquals(13L, cachedVersion(),
                    "DELETE is visible in MS, but daemon table-version equality leaves FE partition version at 12")
        } catch (Throwable failure) {
            logger.error("Lazy commit reproduction failed at stage: ${stage}; DELETE done: ${deleteFuture?.isDone()}",
                    failure)
            throw failure
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
