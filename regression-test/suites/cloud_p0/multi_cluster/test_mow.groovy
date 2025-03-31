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
import groovy.json.JsonSlurper

suite('test_mow', 'multi_cluster,docker') {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
    ]
    options.beConfigs += [
        'enable_debug_points=true',
        'tablet_rowset_stale_sweep_time_sec=0',
    ]

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_params = [string: [:]]

    def triggerCompaction = { tablet ->
        def compact_type = "cumulative"
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        if (compact_type == "cumulative") {
            def (code_1, out_1, err_1) = be_run_cumulative_compaction(be_host, be_http_port, tablet_id)
            logger.info("Run compaction: code=" + code_1 + ", out=" + out_1 + ", err=" + err_1)
            assertEquals(code_1, 0)
            return out_1
        } else if (compact_type == "full") {
            def (code_2, out_2, err_2) = be_run_full_compaction(be_host, be_http_port, tablet_id)
            logger.info("Run compaction: code=" + code_2 + ", out=" + out_2 + ", err=" + err_2)
            assertEquals(code_2, 0)
            return out_2
        } else {
            assertFalse(True)
        }
    }

    def getTabletStatus = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/compaction/show?tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info(command)
        def process = command.execute()
        def code = process.waitFor()
        def out = process.getText()
        logger.info("Get tablet status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def tabletStatus = parseJson(out.trim())
        return tabletStatus
    }

    def waitForCompaction = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        def running = true
        do {
            Thread.sleep(1000)
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X GET http://${be_host}:${be_http_port}")
            sb.append("/api/compaction/run_status?tablet_id=")
            sb.append(tablet_id)

            String command = sb.toString()
            logger.info(command)
            def process = command.execute()
            def code = process.waitFor()
            def out = process.getText()
            logger.info("Get compaction status: code=" + code + ", out=" + out)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)
    }

    def getLocalDeleteBitmapStatus = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        boolean running = true
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/delete_bitmap/count_local?verbose=true&tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info(command)
        def process = command.execute()
        def code = process.waitFor()
        def out = process.getText()
        logger.info("Get local delete bitmap count status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def deleteBitmapStatus = parseJson(out.trim())
        return deleteBitmapStatus
    }

    def getMsDeleteBitmapStatus = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        boolean running = true
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/delete_bitmap/count_ms?verbose=true&tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info(command)
        def process = command.execute()
        def code = process.waitFor()
        def out = process.getText()
        logger.info("Get ms delete bitmap count status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def deleteBitmapStatus = parseJson(out.trim())
        return deleteBitmapStatus
    }

    docker(options) {
        def testTable = "test_mow"

        // add_new_cluster
        def clusterName = "newcluster1"
        cluster.addBackend(1, clusterName)
        def ret = sql_return_maparray """show clusters"""
        def currentCluster = ret.stream().filter(cluster -> cluster.is_current == "TRUE").findFirst().orElse(null)
        def otherCluster = ret.stream().filter(cluster -> cluster.is_current == "FALSE").findFirst().orElse(null)
        assertTrue(otherCluster != null)

        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        GetDebugPoint().enableDebugPointForAllBEs("CumulativeCompaction.modify_rowsets.delete_expired_stale_rowset")

        sql """
            create table ${testTable} (`k` int NOT NULL, `v` int NOT NULL)
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true"
            );
        """

        def tablets = sql_return_maparray """ show tablets from ${testTable}; """
        logger.info("tablets: " + tablets)
        assertEquals(1, tablets.size())
        def tablet = tablets[0]

        sql """ INSERT INTO ${testTable} VALUES (1,99); """
        sql """ INSERT INTO ${testTable} VALUES (1,99); """
        sql """ INSERT INTO ${testTable} VALUES (2,99); """
        sql """ INSERT INTO ${testTable} VALUES (3,99); """
        sql """ INSERT INTO ${testTable} VALUES (4,99); """
        sql "sync"
        order_qt_sql1 """ select * from ${testTable}; """

        sql """use @${otherCluster.cluster}"""
        order_qt_sql2 """ select * from ${testTable}; """

        logger.info("use cluster 0")
        sql """use @${currentCluster.cluster}"""
        sql """ INSERT INTO ${testTable} VALUES (1,100); """
        sql """ INSERT INTO ${testTable} VALUES (2,100); """
        sql """ INSERT INTO ${testTable} VALUES (3,100); """
        sql """ INSERT INTO ${testTable} VALUES (4,100); """
        sql """ INSERT INTO ${testTable} VALUES (5,100); """
        sql """ sync """
        order_qt_sql3 """ select * from ${testTable}; """

        getTabletStatus(tablet)
        assertTrue(triggerCompaction(tablet).contains("Success"))
        waitForCompaction(tablet)
        getTabletStatus(tablet)
        def local_dm = getLocalDeleteBitmapStatus(tablet)
        logger.info("local_dm 0: " + local_dm)
        def ms_dm = getMsDeleteBitmapStatus(tablet)
        logger.info("ms_dm: " + ms_dm)

        logger.info("use cluster 1")
        sql """use @${otherCluster.cluster}"""
        order_qt_sql4 """ select * from ${testTable}; """
        getTabletStatus(tablet)
        local_dm = getLocalDeleteBitmapStatus(tablet)
        logger.info("local_dm 1: " + local_dm)
        ms_dm = getMsDeleteBitmapStatus(tablet)
        logger.info("ms_dm: " + ms_dm)
    }
}
