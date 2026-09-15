-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.

CREATE TABLE TABLE_NAME (
  `_ctime_` datetime(6) NOT NULL,
  `app` varchar(65533) NULL,
  `_namespace_` varchar(65533) NULL,
  `trace_id` text NULL,
  `log_level` text NULL,
  `_pod_name_` text NULL,
  `_node_name_` text NULL,
  `_node_ip_` varchar(15) NULL,
  `_container_name_` text NULL,
  `custom_label` text NULL,
  `msg` text NULL,
  INDEX idx__namespace_ (`_namespace_`) USING INVERTED,
  INDEX idx__ctime_ (`_ctime_`) USING INVERTED,
  INDEX idx_trace_id (`trace_id`) USING INVERTED,
  INDEX idx_log_level (`log_level`) USING INVERTED,
  INDEX idx__pod_name_ (`_pod_name_`) USING INVERTED,
  INDEX idx__node_name_ (`_node_name_`) USING INVERTED,
  INDEX idx__node_ip_ (`_node_ip_`) USING INVERTED,
  INDEX idx__container_name_ (`_container_name_`) USING INVERTED,
  INDEX idx_custom_label (`custom_label`) USING INVERTED PROPERTIES("lower_case"="true", "parser"="unicode", "support_phrase"="true"),
  INDEX idx_msg (`msg`) USING INVERTED PROPERTIES("lower_case"="true", "parser"="unicode", "support_phrase"="true")
) ENGINE=OLAP
DUPLICATE KEY(`_ctime_`, `app`)
AUTO PARTITION BY RANGE (date_trunc(`_ctime_`, 'hour'))
(PARTITION p20260908100000 VALUES [('2026-09-08 10:00:00'), ('2026-09-08 11:00:00')))
DISTRIBUTED BY RANDOM BUCKETS 1
PROPERTIES (
  "file_cache_ttl_seconds"="0",
  "is_being_synced"="false",
  "storage_medium"="hdd",
  "storage_format"="V2",
  "light_schema_change"="true",
  "compaction_policy"="time_series",
  "time_series_compaction_goal_size_mbytes"="2048",
  "time_series_compaction_file_count_threshold"="10000",
  "time_series_compaction_time_threshold_seconds"="36000",
  "time_series_compaction_empty_rowsets_threshold"="5",
  "time_series_compaction_level_threshold"="1",
  "disable_auto_compaction"="false",
  "group_commit_interval_ms"="60000",
  "group_commit_data_bytes"="134217728",
  "inverted_index_storage_format"="V2"
);
