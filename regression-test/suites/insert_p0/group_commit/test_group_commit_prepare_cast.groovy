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

suite("test_group_commit_prepare_cast") {
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, context.dbName, false) +
            "&emulateUnsupportedPstmts=false"
    def configurations = [
            ["off_mode", true],
            ["sync_mode", false],
            ["async_mode", false],
            ["sync_mode", true],
            ["async_mode", true]
    ]

    configurations.each { configuration ->
        def (mode, fullPrepare) = configuration
        def tag = "${mode}_${fullPrepare}"

        // Each scenario gets a fresh connection and retains one server-side handle across SETs.
        lazyCheck {
            sql "DROP TABLE IF EXISTS test_group_commit_prepare_timezone"
            sql """
                CREATE TABLE test_group_commit_prepare_timezone (id INT, v DATETIME(3))
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num" = "1", "group_commit_interval_ms" = "100")
            """
            connect(user, password, url) {
                sql "SET group_commit = '${mode}'"
                sql "SET enable_group_commit_full_prepare = ${fullPrepare}"
                sql "SET enable_insert_strict = false"
                sql "SET enable_strict_cast = false"
                sql "SET time_zone = '+00:00'"
                def stmt = prepareStatement """
                    INSERT INTO test_group_commit_prepare_timezone VALUES (?, CAST(? AS DATETIME(3)))
                """
                try {
                    ["+08:00", "-05:00", "+00:00"].eachWithIndex { zone, index ->
                        sql "SET time_zone = '${zone}'"
                        stmt.setInt(1, index + 1)
                        stmt.setString(2, "2024-01-01 00:00:00+00:00")
                        stmt.executeUpdate()
                    }
                } finally {
                    stmt.close()
                }
            }
            // Read on a separate connection and wait for async Group Commit publication.
            connect(user, password, url) {
                awaitUntil(60) {
                    sql("SELECT COUNT(*) FROM test_group_commit_prepare_timezone")[0][0] == 3
                }
                // Expected: 2024-01-01 08:00:00, 2023-12-31 19:00:00, 2024-01-01 00:00:00.
                quickTest("timezone_${tag}", """
                    SELECT id, CAST(v AS STRING) FROM test_group_commit_prepare_timezone ORDER BY id
                """)
            }
        }

        lazyCheck {
            sql "DROP TABLE IF EXISTS test_group_commit_prepare_strict"
            sql """
                CREATE TABLE test_group_commit_prepare_strict (id INT, v INT NULL)
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num" = "1", "group_commit_interval_ms" = "100")
            """
            connect(user, password, url) {
                sql "SET group_commit = '${mode}'"
                sql "SET enable_group_commit_full_prepare = ${fullPrepare}"
                sql "SET enable_insert_strict = false"
                sql "SET enable_strict_cast = false"
                def stmt = prepareStatement """
                    INSERT INTO test_group_commit_prepare_strict VALUES (?, CAST(? AS INT))
                """
                try {
                    stmt.setInt(1, 1)
                    stmt.setString(2, "bad")
                    stmt.executeUpdate()

                    // Change only CAST strictness: row 1 stays NULL, but row 2 must be rejected.
                    sql "SET enable_strict_cast = true"
                    stmt.setInt(1, 2)
                    stmt.setString(2, "bad")
                    lazyCheck {
                        test {
                            sql stmt
                            exception "bad"
                        }
                    }

                    // The same handle must remain usable after the failed execution.
                    stmt.setInt(1, 3)
                    stmt.setString(2, "42")
                    stmt.executeUpdate()
                } finally {
                    stmt.close()
                }
            }
            connect(user, password, url) {
                awaitUntil(60) {
                    sql("SELECT COUNT(*) FROM test_group_commit_prepare_strict WHERE id IN (1, 3)")[0][0] == 2
                }
                // Expected: (1, NULL), (3, 42); changing strictness never rewrites row 1.
                quickTest("strict_${tag}", """
                    SELECT id, v FROM test_group_commit_prepare_strict ORDER BY id
                """)
            }
        }

        // These statements stay on the full-prepare path: SET must invalidate its cached load settings.
        lazyCheck {
            sql "DROP TABLE IF EXISTS test_group_commit_prepare_raw_strict"
            sql """
                CREATE TABLE test_group_commit_prepare_raw_strict (id INT, v INT NULL)
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num" = "1", "group_commit_interval_ms" = "100")
            """
            connect(user, password, url) {
                sql "SET group_commit = '${mode}'"
                sql "SET enable_group_commit_full_prepare = ${fullPrepare}"
                sql "SET enable_insert_strict = false"
                sql "SET enable_strict_cast = false"
                def stmt = prepareStatement "INSERT INTO test_group_commit_prepare_raw_strict VALUES (?, ?)"
                try {
                    stmt.setInt(1, 1)
                    stmt.setString(2, "bad")
                    stmt.executeUpdate()

                    sql "SET enable_strict_cast = true"
                    stmt.setInt(1, 2)
                    lazyCheck {
                        test {
                            sql stmt
                            exception "bad"
                        }
                    }
                    stmt.setInt(1, 3)
                    stmt.setString(2, "42")
                    stmt.executeUpdate()

                    // The rebuilt strict plan must also reject bad input when reused without SET.
                    stmt.setInt(1, 4)
                    stmt.setString(2, "bad")
                    lazyCheck {
                        test {
                            sql stmt
                            exception "bad"
                        }
                    }
                } finally {
                    stmt.close()
                }
            }
            connect(user, password, url) {
                awaitUntil(60) {
                    sql("SELECT COUNT(*) FROM test_group_commit_prepare_raw_strict WHERE id IN (1, 3)")[0][0] == 2
                }
                quickTest("raw_strict_${tag}", """
                    SELECT id, v FROM test_group_commit_prepare_raw_strict ORDER BY id
                """)
            }
        }
    }
}
