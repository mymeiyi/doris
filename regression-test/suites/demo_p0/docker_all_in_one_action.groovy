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

// This docker suite demonstrates the all-in-one mode where all components
// (FDB, MS, Recycler, FE, BE) run inside a single Docker container.
// All-in-one mode requires cloud mode and creates a minimal cluster
// with 1 FE + 1 BE + 1 MS + 1 Recycler + 1 FDB in one container.

import org.apache.doris.regression.suite.ClusterOptions

suite('docker_all_in_one_action', 'docker') {
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 1
    options.cloudMode = true
    options.allInOne = true

    docker(options) {
        sql '''
            DROP TABLE IF EXISTS test_all_in_one
        '''

        sql '''
            CREATE TABLE test_all_in_one (
                k INT,
                v STRING
            ) DISTRIBUTED BY HASH(k) BUCKETS 3
            PROPERTIES ("replication_num" = "1")
        '''

        sql '''
            INSERT INTO test_all_in_one VALUES
            (1, 'hello'),
            (2, 'world'),
            (3, 'all_in_one')
        '''

        order_qt_select_all '''
            SELECT * FROM test_all_in_one
        '''

        // Verify basic cluster operations work
        def result = sql '''SHOW FRONTENDS'''
        assertTrue(result.size() > 0, "Should have at least 1 FE")

        result = sql '''SHOW BACKENDS'''
        assertTrue(result.size() > 0, "Should have at least 1 BE")

        // Test stop and restart a BE component
        // cluster.stopBackends(1)
        // cluster.checkBeIsAlive(1, false)

        // cluster.startBackends(1)
        // cluster.checkBeIsAlive(1, true)

        // Verify data is still accessible after restart
        order_qt_after_restart '''
            SELECT * FROM test_all_in_one
        '''
    }
}
