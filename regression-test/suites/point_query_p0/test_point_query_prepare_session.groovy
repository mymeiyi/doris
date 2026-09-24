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

suite("test_point_query_prepare_session") {
    sql "DROP TABLE IF EXISTS test_point_query_prepare_session"
    sql """
        CREATE TABLE test_point_query_prepare_session (id INT NOT NULL, v VARCHAR(20))
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1", "store_row_column" = "true",
                   "enable_unique_key_merge_on_write" = "true")
    """
    sql "INSERT INTO test_point_query_prepare_session VALUES (1, 'bad'), (2, '42')"
    def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, context.dbName, false) +
            "&emulateUnsupportedPstmts=false"
    connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
        sql "SET enable_strict_cast = false"
        explain {
            sql "SELECT CAST(v AS INT) FROM test_point_query_prepare_session WHERE id = 1"
            contains "SHORT-CIRCUIT"
        }
        def stmt = prepareStatement "SELECT CAST(v AS INT) FROM test_point_query_prepare_session WHERE id = ?"
        try {
            // Every query returns at most one row because id is the complete unique key.
            stmt.setInt(1, 1)
            qe_non_strict stmt
            stmt.setInt(1, 2)
            qe_cached_non_strict stmt

            // Replan on the same handle and replace the BE expression cache UUID as well.
            sql "SET enable_strict_cast = true"
            stmt.setInt(1, 1)
            test {
                sql stmt
                exception "bad"
            }
            stmt.setInt(1, 2)
            qe_strict_valid stmt
            stmt.setInt(1, 1)
            test {
                sql stmt
                exception "bad"
            }

            sql "SET enable_strict_cast = false"
            stmt.setInt(1, 1)
            qe_non_strict_again stmt
            qe_cached_non_strict_again stmt
        } finally {
            stmt.close()
        }
    }
}
