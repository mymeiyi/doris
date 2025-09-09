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

package org.apache.doris.cloud.catalog;

import org.apache.doris.common.io.CountingDataOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class CloudSnapshotEnv extends CloudEnv {

    private static final Logger LOG = LogManager.getLogger(CloudSnapshotEnv.class);

    public CloudSnapshotEnv(boolean isCheckpointCatalog) {
        super(isCheckpointCatalog);
    }

    @Override
    public long saveMasterInfo(CountingDataOutputStream dos, long checksum) throws IOException {
        LOG.info("skip save masterInfo");
        return checksum;
    }

    @Override
    public long saveFrontends(CountingDataOutputStream dos, long checksum) throws IOException {
        LOG.info("skip save frontends");
        return checksum;
    }

    @Override
    public long saveBackends(CountingDataOutputStream dos, long checksum) throws IOException {
        LOG.info("skip save backends");
        return checksum;
    }

    public void setReplayedJournalId(long journalId) {
        this.replayedJournalId = new AtomicLong(journalId);
    }
}
