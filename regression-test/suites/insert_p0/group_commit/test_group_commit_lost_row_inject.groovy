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

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement

// Deterministically reproduce the group-commit lost-row race.
//
// In production the race appears with server-side prepared statements that reuse one group
// commit plan: every insert RPC then shares one load_id, and LoadBlockQueue._load_ids_to_write_dep
// (a map keyed by load_id) is corrupted -- the 2nd add_load_id overwrites the 1st, and a single
// remove_load_id from whichever load finishes first clears the shared entry, so the internal
// consumer sees "no writer in flight" and commits while the other load is still about to append
// its block. That late block is appended to an already-committed queue and is silently lost.
//
// A single MySQL connection processes a prepared-statement batch row-by-row (FE blocks on each
// RPC response), so the two inserts never overlap there. To reproduce reliably we instead use
// TWO concurrent connections plus two BE debug points:
//   * GroupCommitBlockSink.force_shared_load_id : force the sink load_id to a constant so the two
//     independent loads collide in the same block queue, exactly as plan reuse does.
//   * LoadBlockQueue.add_block.block_second_load : barrier the 1st add_block until a 2nd has also
//     arrived at the queue, then block that 2nd add_block so the 1st load finishes and the consumer
//     commits first; when released the 2nd block lands on the already-committed queue and is lost.
//
// Requires the BE debug-point patch in be/src/load/group_commit and be/src/exec/operator.
//
// Result:
//   * current (buggy) code -> only 1 of the 2 rows committed: count == 1  (assertion passes -> bug reproduced)
//   * after the fix        -> both rows committed:            count == 2  (this assertion then fails)
suite("test_group_commit_lost_row_inject", "nonConcurrent") {
    def dbName = "regression_test_insert_p0"
    def table = dbName + ".test_group_commit_lost_row_inject"
    def dpForce = "GroupCommitBlockSink.force_shared_load_id"
    def dpBlock = "LoadBlockQueue.add_block.block_second_load"

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "drop table if exists ${table}"
    sql """
        CREATE TABLE ${table} (
            `id` int(11) NOT NULL,
            `name` varchar(50) NULL,
            `score` int(11) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`, `name`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "group_commit_interval_ms" = "1000",
            "replication_num" = "1"
        );
    """

    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    // url pointing at our db with async group commit
    def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName, false) + "&sessionVariables=group_commit=async_mode"
    logger.info("jdbc url: " + url)

    def errs = new java.util.concurrent.CopyOnWriteArrayList<String>()
    def doInsert = { String vals ->
        Connection conn = null
        try {
            conn = DriverManager.getConnection(url, user, password)
            Statement st = conn.createStatement()
            st.execute("insert into ${table} values ${vals}")
            st.close()
            logger.info("insert ${vals} returned ok")
        } catch (Throwable e) {
            errs.add(vals + " -> " + e.getMessage())
            logger.info("insert ${vals} threw (may be expected when the late block is dropped): " + e.getMessage())
        } finally {
            if (conn != null) {
                try { conn.close() } catch (Throwable ignore) {}
            }
        }
    }

    try {
        GetDebugPoint().clearDebugPointsForAllBEs()
        GetDebugPoint().enableDebugPointForAllBEs(dpForce)
        GetDebugPoint().enableDebugPointForAllBEs(dpBlock)

        // A: creates the block queue and registers the forced load_id, reaches add_block (arrival #1),
        //    then waits at the barrier for a 2nd arrival.
        def tA = Thread.start { doInsert("(2, 'b', 20)") }
        // give A time to create the queue + register the shared load_id before B joins it
        sleep(500)
        // B: joins the same queue (shared load_id), reaches add_block (arrival #2) -> releases A,
        //    then B is the one that gets blocked.
        def tB = Thread.start { doInsert("(3, 'c', 30)") }

        // While B is blocked: A appends its block, removes the (shared) load_id, and the consumer
        // commits the queue (interval = 1s) with only A's row.
        sleep(3000)

        // Release B: its block is appended to the already-committed queue and is lost.
        GetDebugPoint().disableDebugPointForAllBEs(dpBlock)
        tA.join()
        tB.join()
        GetDebugPoint().disableDebugPointForAllBEs(dpForce)

        logger.info("insert errors: " + errs)

        def cnt = 0
        def rows = null
        for (int i = 0; i < 15; i++) {
            sleep(2000)
            rows = sql "select id, name, score from ${table} order by id"
            cnt = rows.size()
            logger.info("rows = " + rows + ", retry = " + i)
            if (cnt >= 2) break
        }

        // FIXED: both rows committed -> count == 2.
        // If the lost-row race regresses, one row is dropped -> count == 1 and this fails.
        assertEquals(2, cnt)
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
