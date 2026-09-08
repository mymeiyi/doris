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

suite("test_cloud_duplicate_memtable_on_sink", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(3)

    docker(options) {
        sql "DROP TABLE IF EXISTS test_cloud_duplicate_memtable_on_sink_source"
        sql "DROP TABLE IF EXISTS test_cloud_duplicate_memtable_on_sink"

        sql """
            CREATE TABLE test_cloud_duplicate_memtable_on_sink_source (
                k BIGINT NOT NULL,
                v BIGINT NOT NULL
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 3
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            CREATE TABLE test_cloud_duplicate_memtable_on_sink (
                k BIGINT NOT NULL,
                v BIGINT NOT NULL
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """

        sql "SET enable_memtable_on_sink_node = false"
        sql """
            INSERT INTO test_cloud_duplicate_memtable_on_sink_source
            SELECT number, number * 2 FROM numbers("number" = "100000")
        """

        try {
            sql "SET enable_memtable_on_sink_node = true"
            sql "SET profile_level = 2"
            profile("cloud_duplicate_memtable_on_sink_profile") {
                run {
                    sql """
                        /* cloud_duplicate_memtable_on_sink_profile */
                        INSERT INTO test_cloud_duplicate_memtable_on_sink
                        SELECT k, v FROM test_cloud_duplicate_memtable_on_sink_source
                    """
                    sleep(500)
                }
                check { profileString, exception ->
                    logger.info("memtable-on-sink profile:\n{}", profileString)
                    if (exception != null) {
                        throw exception
                    }
                    assertTrue(profileString.contains("DeltaWriterV2"))
                }
            }
        } finally {
            sql "SET enable_memtable_on_sink_node = false"
        }

        sql """
            SELECT assert_true(
                COUNT(*) = 100000
                    AND SUM(k) = 4999950000
                    AND SUM(v) = 9999900000,
                'cloud duplicate memtable-on-sink result mismatch')
            FROM test_cloud_duplicate_memtable_on_sink
        """
    }
}
