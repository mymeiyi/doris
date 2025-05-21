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


import groovyjarjarantlr4.v4.codegen.model.ExceptionClause

import java.util.Date
import java.text.SimpleDateFormat
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpPut
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.RedirectStrategy
import org.apache.http.protocol.HttpContext
import org.apache.http.HttpRequest
import org.apache.http.impl.client.LaxRedirectStrategy
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.util.EntityUtils
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_group_commit", "p0") {
    def tableName3 = "test_group_commit"

    def getJobState = { tableName ->
        def jobStateResult = sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        logger.info("jobStateResult: ${jobStateResult}")
        return jobStateResult[0][9]
    }

    def getCreateViewState = { tableName ->
        def createViewStateResult = sql """ SHOW ALTER TABLE MATERIALIZED VIEW WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return createViewStateResult[0][8]
    }

    def execStreamLoad = {
        streamLoad {
            table "${tableName3}"

            set 'column_separator', ','

            file 'all_types.csv'
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                // assertEquals("success", json.Status.toLowerCase())
                // assertEquals(2500, json.NumberTotalRows)
                // assertEquals(0, json.NumberFilteredRows)
            }
        }
    }

    onFinish {
        GetDebugPoint().clearDebugPointsForAllBEs()
        GetDebugPoint().clearDebugPointsForAllFEs()
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    sql """ DROP TABLE IF EXISTS ${tableName3} """

    sql """
        CREATE TABLE ${tableName3} (
                `id` int(11) NOT NULL,
                `name` varchar(50) NULL,
                `score` int(11) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "group_commit_interval_ms" = "200"
            );
    """


    // CountDownLatch latch = new CountDownLatch(1)
    def insert = {
        sql """ set group_commit = async_mode; """
        sql """ insert into ${tableName3} values (1, 'a', 100) """
        // latch.countDown()
    }

    GetDebugPoint().enableDebugPointForAllFEs("FE.FrontendServiceImpl.initHttpStreamPlan.block")
    GetDebugPoint().enableDebugPointForAllFEs("FE.SchemaChangeJobV2.createShadowIndexReplica.addShadowIndexToCatalog.block")
    // sql """ set group_commit = aysnc_mode; """
    // sql """ insert into ${tableName3} values (1, 'a', 1) """
    // execStreamLoad()
    Thread thread = new Thread(() -> insert())
    thread.start()
    sleep(1000)
    def result = sql "select count(*) from ${tableName3}"
    logger.info("table: ${tableName3}, rowCount: ${result}")
    assertEquals(0, result[0][0])

    // sql """ alter table ${tableName3} modify column score int NULL"""
    sql """ alter table ${tableName3} order by(id, score, name) """
    GetDebugPoint().enableDebugPointForAllFEs("FE.SchemaChangeJobV2.runRunning.block")
    GetDebugPoint().disableDebugPointForAllFEs("FE.SchemaChangeJobV2.createShadowIndexReplica.addShadowIndexToCatalog.block")
    sleep(2000)
    getJobState(tableName3)
    GetDebugPoint().disableDebugPointForAllFEs("FE.FrontendServiceImpl.initHttpStreamPlan.block")

    def getRowCount = { expectedRowCount ->
        Awaitility.await().atMost(90, SECONDS).pollInterval(1, SECONDS).until(
                {
                    result = sql "select count(*) from ${tableName3}"
                    logger.info("table: ${tableName3}, rowCount: ${result}")
                    return result[0][0] == expectedRowCount
                }
        )
    }

    getRowCount(1)
    qt_sql """ select id, name, score from ${tableName3} """
}

