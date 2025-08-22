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

suite("test_delete_bitmap_v2", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def configs = [
            // 0: write v1, read v1
            ['enable_sync_tablet_delete_bitmap_by_cache'      : 'false',
             'delete_bitmap_store_write_version'              : '1',
             'delete_bitmap_store_read_version'               : '1',
             'enable_delete_bitmap_store_v2_check_correctness': 'false'
            ],
            // 1: write v2, read v2, store in fdb, check correctness
            ['enable_sync_tablet_delete_bitmap_by_cache'      : 'false',
             'delete_bitmap_store_write_version'              : '2',
             'delete_bitmap_store_read_version'               : '2',
             'delete_bitmap_store_v2_max_bytes_in_fdb'        : '-1',
             'enable_delete_bitmap_store_v2_check_correctness': 'true'
            ],
            // 2: write v2, read v2, store in s3, check correctness
            ['enable_sync_tablet_delete_bitmap_by_cache'      : 'false',
             'delete_bitmap_store_write_version'              : '2',
             'delete_bitmap_store_read_version'               : '2',
             'delete_bitmap_store_v2_max_bytes_in_fdb'        : '0',
             'enable_delete_bitmap_store_v2_check_correctness': 'true'
            ],
            // 3: write v2, read v2, store in fdb or s3, check correctness
            ['enable_sync_tablet_delete_bitmap_by_cache'      : 'false',
             'delete_bitmap_store_write_version'              : '2',
             'delete_bitmap_store_read_version'               : '2',
             'delete_bitmap_store_v2_max_bytes_in_fdb'        : '1',
             'enable_delete_bitmap_store_v2_check_correctness': 'true'
            ],
            // 4: write v3, read v3, store in fdb or s3, check correctness
            ['enable_sync_tablet_delete_bitmap_by_cache'      : 'false',
             'delete_bitmap_store_write_version'              : '3',
             'delete_bitmap_store_read_version'               : '3',
             'delete_bitmap_store_v2_max_bytes_in_fdb'        : '1',
             'enable_delete_bitmap_store_v2_check_correctness': 'true'
            ],
            // 5: write v3, read v1, store in fdb or s3, check correctness
            ['enable_sync_tablet_delete_bitmap_by_cache'      : 'false',
             'delete_bitmap_store_write_version'              : '3',
             'delete_bitmap_store_read_version'               : '1',
             'delete_bitmap_store_v2_max_bytes_in_fdb'        : '1',
             'enable_delete_bitmap_store_v2_check_correctness': 'true'
            ],
            // 6: write v3, read v2, store in fdb or s3, check correctness
            ['enable_sync_tablet_delete_bitmap_by_cache'      : 'false',
             'delete_bitmap_store_write_version'              : '3',
             'delete_bitmap_store_read_version'               : '2',
             'delete_bitmap_store_v2_max_bytes_in_fdb'        : '1',
             'enable_delete_bitmap_store_v2_check_correctness': 'true'
            ],
    ]

    for (int i = 0; i < configs.size(); i++) {
        def config = configs[i]
        config.each { k, v ->
            update_all_be_config(k, v)
        }

        def table_name = "test_delete_bitmap_v2_" + i
        sql """ drop table if exists ${table_name} force; """
        sql """
            CREATE TABLE ${table_name} (
                `k` int(11) NOT NULL,
                `v` int(11) NOT NULL
            ) ENGINE=OLAP
            unique KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "disable_auto_compaction"="true",
                "replication_num" = "1"
            ); 
        """
        def tablets = sql_return_maparray """ show tablets from ${table_name} """
        assertEquals(1, tablets.size())
        def tablet = tablets[0]
        logger.info("table: ${table_name}, tablets: ${tablet['TabletId']}" )

        order_qt_select_0 "SELECT ${table_name};"

        sql """ insert into ${table_name} values(1, 1), (2, 2); """
        sql """ insert into ${table_name} values(3, 3), (4, 4); """
        sql """ insert into ${table_name} values(1, 10), (3, 30); """
        order_qt_select_1 "SELECT * FROM ${table_name};"

        sql """ insert into ${table_name} values(2, 20), (4, 40); """
        order_qt_select_2 "SELECT * FROM ${table_name};"

        sql """ drop table if exists ${table_name} force; """
    }
}
