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

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.snapshot.SnapshotState;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.EditLogFileOutputStream;
import org.apache.doris.persist.Storage;
import org.apache.doris.rpc.RpcException;

import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class CloudSnapshotHandler extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(CloudSnapshotHandler.class);
    private LinkedBlockingQueue<CloudSnapshotJob> needScheduleJobs = Queues.newLinkedBlockingQueue();
    public static final String SNAPSHOT_DIR = "/snapshot";
    private String snapshotDir;

    public CloudSnapshotHandler() {
        super("cloud snapshot handler", Config.cloud_snapshot_handler_interval_second * 1000);
        this.snapshotDir = Config.meta_dir + SNAPSHOT_DIR;
    }

    public void initialize() {
        // TODO delete unused snapshot files if fe restarts
        File imageDir = new File(this.snapshotDir);
        if (!imageDir.exists()) {
            imageDir.mkdirs();
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of CloudSnapshot with error message {}", e.getMessage(), e);
        }
    }

    public void submitJob(CloudSnapshotJob job) {
        needScheduleJobs.add(job);
    }

    private void process() {
        while (true) {
            if (needScheduleJobs.isEmpty()) {
                return;
            }
            CloudSnapshotJob job = needScheduleJobs.poll();
            try {
                execute(job);
            } catch (Exception e) {
                LOG.warn("cloud snapshot job failed", e);
            }
        }
    }

    private void execute(CloudSnapshotJob job) throws Exception {
        Cloud.BeginSnapshotResponse response = beginSnapshot(job);
        String snapshotId = response.getSnapshotId();
        String imageUrl = response.getImageUrl();
        Cloud.ObjectStoreInfoPB objInfo = response.getObjInfo();
        // 1. write edit log
        SnapshotState snapshotState = new SnapshotState(snapshotId, imageUrl);
        long logId = Env.getCurrentEnv().getEditLog().logBeginSnapshot(snapshotState);
        // 2. upload image
        uploadImage(snapshotId, imageUrl, objInfo, logId);
    }

    private Cloud.BeginSnapshotResponse beginSnapshot(CloudSnapshotJob job) throws Exception {
        Cloud.BeginSnapshotRequest.Builder builder = Cloud.BeginSnapshotRequest.newBuilder()
                .setTimeoutSeconds(Config.cloud_snapshot_timeout_seconds).setAutoSnapshot(job.isAuto())
                .setTtlSeconds(job.getTtl());
        if (job.getLabel() != null) {
            builder.setSnapshotLabel(job.getLabel());
        }
        try {
            Cloud.BeginSnapshotResponse response = MetaServiceProxy.getInstance().beginSnapshot(builder.build());
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("beginSnapshot response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
            return response;
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }

    private void uploadImage(String snapshotId, String imageUrl, Cloud.ObjectStoreInfoPB objInfo, long logId)
            throws Exception {
        LOG.info("Start to snapshotId: {}, imageUrl: {}, logId: {}", snapshotId, imageUrl, logId);
        // scan edit logs between imageVersion + 1 and logId
        long imageVersion = getImageVersion();
        if (imageVersion + 1 < logId) {
            writeSnapshotEditLogFile(imageVersion + 1, logId, snapshotId);
        }
        // use lock to prevent checkpoint
        // upload image files
        String imageDir = Env.getServingEnv().getImageDir();
        String imageFileName = "image." + imageVersion;
        File imageFile = new File(imageDir + "/" + imageFileName);
        if (!imageFile.exists()) {
            LOG.error("image file does not exist: {}", imageFile.getAbsoluteFile());
            throw new DdlException("image file does not exist: " + imageFile.getAbsoluteFile());
        }
        RemoteBase.ObjectInfo objectInfo = new RemoteBase.ObjectInfo(objInfo);
        RemoteBase remote = RemoteBase.newInstance(objectInfo);
        remote.putObject(imageFile, imageUrl + "/" + imageFileName);
        // edit log
        File snapshotEditLogFile = new File(snapshotDir, snapshotId);
        remote.putObject(snapshotEditLogFile, imageUrl + "/" + snapshotEditLogFile.getName());
        snapshotEditLogFile.delete();
    }

    private long getImageVersion() throws DdlException {
        try {
            Storage storage = new Storage(Env.getServingEnv().getImageDir());
            return storage.getLatestImageSeq();
        } catch (Throwable e) {
            LOG.warn("get image version failed", e);
            throw new DdlException("get image version failed: " + e.getMessage());
        }
    }

    private void writeSnapshotEditLogFile(long fromJournalId, long toJournalId, String snapshotId)
            throws IOException, DdlException {
        LOG.info("scan journal from {} to {} for snapshotId: {}", fromJournalId, toJournalId, snapshotId);
        JournalCursor cursor = Env.getCurrentEnv().getEditLog().read(fromJournalId, toJournalId);
        if (cursor == null) {
            LOG.warn("failed to get cursor from {} to {}", fromJournalId, toJournalId);
            throw new DdlException("failed to get cursor from " + fromJournalId + " to " + toJournalId);
        }

        File snapshotEditLogFile = new File(snapshotDir, snapshotId);
        if (snapshotEditLogFile.exists()) {
            snapshotEditLogFile.delete();
        }
        snapshotEditLogFile.createNewFile();
        EditLogFileOutputStream outputStream = null;
        try {
            outputStream = new EditLogFileOutputStream(snapshotEditLogFile);
            while (true) {
                Pair<Long, JournalEntity> kv = cursor.next();
                if (kv == null) {
                    break;
                }
                // Long logId = kv.first;
                JournalEntity entity = kv.second;
                if (entity == null) {
                    break;
                }
                outputStream.write(entity.getOpCode(), entity.getData());
            }
            outputStream.setReadyToFlush();
            outputStream.flush();
            outputStream.close();
        } catch (Exception e) {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException ex) {
                    LOG.warn("failed to close output stream", ex);
                }
            }
            try {
                if (snapshotEditLogFile.exists()) {
                    snapshotEditLogFile.delete();
                }
            } catch (Exception ex) {
                LOG.warn("failed to delete snapshot file", ex);
            }
            throw new DdlException(e.getMessage());
        }
    }
}
