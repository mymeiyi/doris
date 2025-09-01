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

package org.apache.doris.cloud.load;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CloudSnapshotJob  {

    private static final Logger LOG = LogManager.getLogger(CloudSnapshotJob.class);

    private long ttl;
    private String label;
    private boolean auto;

    public CloudSnapshotJob(long ttl, String label, boolean auto) {
        this.ttl = ttl;
        this.label = label;
        this.auto = auto;
    }


    public long getTtl() {
        return ttl;
    }

    public String getLabel() {
        return label;
    }

    public boolean isAuto() {
        return auto;
    }
}
