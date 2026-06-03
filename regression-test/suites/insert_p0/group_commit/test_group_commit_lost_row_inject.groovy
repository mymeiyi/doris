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

import java.sql.PreparedStatement
import com.mysql.cj.jdbc.StatementImpl

// Deterministically reproduce the group-commit lost-row race that happens when several
// inserts of the SAME server-side prepared statement reuse one group commit plan and thus
// share one load_id.
//
// Root cause: LoadBlockQueue._load_ids_to_write_dep is a map keyed by load_id. When two
// concurrent inserts share the load_id and join the same block queue, the second add_load_id
// overwrites the first's entry, and a single remove_load_id from whichever load finishes
// first clears the shared entry. The internal consumer then sees "no writer in flight"
// (_load_ids_to_write_dep.empty()) and commits, while the other load's block is appended
// afterwards to the already-committed queue and is silently lost (add_block has no
// _need_commit / committed guard and returns OK, so FE thinks the row succeeded).
//
// This test injects the BE debug point "LoadBlockQueue.add_block.block_second_load" to force
// the exact ordering. It REQUIRES the debug point patch in be/src/load/group_commit/.
//
// Expected result:
//   - current (buggy) code  -> only 2 rows committed (id=1 + one of id=2/id=3): assertion fails
//   - after the fix         -> all 3 rows committed (id=1,2,3): assertion passes
suite("test_group_commit_lost_row_inject") {
    def dbName = "regression_test_insert_p0"
    def table = dbName + ".test_group_commit_lost_row_inject"
    def dp = "LoadBlockQueue.add_block.block_second_load"

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    try {
        GetDebugPoint().clearDebugPointsForAllBEs()

        def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName, false)
        logger.info("server-prepare url: " + url)

        connect(context.config.jdbcUser, context.config.jdbcPassword,
                url + "&sessionVariables=group_commit=async_mode") {
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

            PreparedStatement stmt = prepareStatement "INSERT INTO ${table} VALUES(?, ?, ?)"
            assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt.class)

            // 1) Warm up: the first executeBatch builds the reusable group commit plan, fixing
            //    the shared load_id L for all subsequent inserts of this prepared statement.
            stmt.setInt(1, 1); stmt.setString(2, "a"); stmt.setInt(3, 10); stmt.addBatch()
            stmt.executeBatch()
            // Let the warm-up queue commit (interval = 1s) so it does not interfere below.
            sleep(3000)

            // 2) Inject: barrier the 1st add_block until a 2nd load joins the same queue, then
            //    block that 2nd add_block until we disable the debug point.
            GetDebugPoint().enableDebugPointForAllBEs(dp)

            // 3) One executeBatch with two rows. They reuse the plan => share load_id L, the
            //    driver pipelines them, and both join the same new block queue. The 2nd row's
            //    add_block blocks in BE, so run executeBatch in a background thread. (Create
            //    the batch here, only the blocking executeBatch runs in the thread.)
            stmt.setInt(1, 2); stmt.setString(2, "b"); stmt.setInt(3, 20); stmt.addBatch()
            stmt.setInt(1, 3); stmt.setString(2, "c"); stmt.setInt(3, 30); stmt.addBatch()
            def batchErr = null
            def t = Thread.start {
                try {
                    def r = stmt.executeBatch()
                    logger.info("batch2 result: " + r)
                } catch (Throwable e) {
                    batchErr = e
                    logger.info("batch2 threw (may be expected when the late block is dropped): "
                            + e.getMessage())
                }
            }

            // 4) While the 2nd add_block is blocked, the 1st row is appended, its (shared)
            //    load_id is removed, and the consumer commits the queue (interval = 1s) with
            //    only 1 row.
            sleep(3000)

            // 5) Release the blocked add_block: the late block is appended to the
            //    already-committed queue and is lost.
            GetDebugPoint().disableDebugPointForAllBEs(dp)
            t.join()

            // 6) Verify. Retry a bit to let everything settle.
            def cnt = 0
            for (int i = 0; i < 10; i++) {
                sleep(2000)
                def c = sql "select count(*) from ${table}"
                cnt = c[0][0]
                logger.info("count = " + cnt + ", retry = " + i)
                if (cnt >= 3) {
                    break
                }
            }
            def rows = sql "select id, name, score from ${table} order by id"
            logger.info("final rows = " + rows)

            // BUG: one of id=2/id=3 is silently lost -> count == 2.
            // FIXED: all three rows are committed -> count == 3.
            assertEquals(3, cnt)
        }
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
