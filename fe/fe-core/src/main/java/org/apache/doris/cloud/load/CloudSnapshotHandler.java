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

import com.google.common.collect.Queues;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.snapshot.SnapshotState;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.EditLogFileOutputStream;
import org.apache.doris.persist.Storage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class CloudSnapshotHandler extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(CloudSnapshotHandler.class);

    public static final String SNAPSHOT_DIR = "/snapshot";

    private LinkedBlockingQueue<CleanCopyJobTask> needScheduleJobs = Queues.newLinkedBlockingQueue();
    private String snapshotDir;

    public CloudSnapshotHandler() {
        super("cloud snapshot handler", Config.cloud_snapshot_handler_interval_second * 1000);
        this.snapshotDir = Config.meta_dir + SNAPSHOT_DIR;
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of CloudSnapshot with error message {}", e.getMessage(), e);
        }
    }

    private void process() {
        while (true) {
            if (needScheduleJobs.isEmpty()) {
                return;
            }
            CleanCopyJobTask task = needScheduleJobs.poll();
            try {
                task.execute();
            } catch (Exception e) {
                LOG.warn("Failed clean copy job", e);
            }
        }
    }

    public void snapshot(String snapshotId, String snapshotUrl) throws Exception {
        String prefix = "meiyi";
        snapshotUrl = prefix + "/snapshot/" + System.currentTimeMillis() + "/";
        LOG.info("Start to snapshot {}", snapshotUrl);
        SnapshotState snapshotState = new SnapshotState(snapshotId, snapshotUrl);
        long logId = Env.getCurrentEnv().getEditLog().logBeginSnapshot(snapshotState);
        // scan edit logs between imageVersion and logId
        long imageVersion = getImageVersion();
        if (imageVersion == -1) {
            return;
        }
        if (imageVersion < logId) {
            scanJournal(imageVersion, logId, snapshotId);
        }
        // use lock to prevent checkpoint
        // upload image files
        String imageDir = Env.getServingEnv().getImageDir();
        // upload edit log file
        // ObjStorage objStorage = new ObjStorage(snapshotId, snapshotUrl, imageDir);
        String imageFileName = "image." + imageVersion;
        File imageFile = new File(imageDir + "/" + imageFileName);
        if (!imageFile.exists()) {
            LOG.error("image file does not exist: {}", imageFile.getAbsoluteFile());
            return;
        }
        Cloud.ObjectStoreInfoPB.Provider provider = Cloud.ObjectStoreInfoPB.Provider.COS;
        String ak = Config.ak;
        String sk = Config.sk;
        String bucket = Config.bucket;
        String endpoint = "cos.ap-beijing.myqcloud.com";
        String region = "ap-beijing";

        RemoteBase.ObjectInfo objectInfo = new RemoteBase.ObjectInfo(provider, ak, sk, bucket, endpoint, region, prefix);
        RemoteBase remote = RemoteBase.newInstance(objectInfo);
        remote.putObject(imageFile, snapshotUrl + "/" + imageFileName);
        // delete edit log

        // test load image
    }

    public long getImageVersion() {
        try {
            Storage storage = new Storage(Env.getServingEnv().getImageDir());
            return storage.getLatestImageSeq();
        } catch (Throwable e) {
            LOG.warn("Save image failed: " + e.getMessage(), e);
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_IMAGE_WRITE_FAILED.increase(1L);
            }
            return -1;
        }
    }

    public synchronized boolean scanJournal(long fromJournalId, long newToJournalId, String snapshotId) throws IOException {
        LOG.info("scan journal id is {}, replay to journal id is {}", fromJournalId, newToJournalId);
        JournalCursor cursor = Env.getCurrentEnv().getEditLog().read(fromJournalId + 1, newToJournalId);
        if (cursor == null) {
            LOG.warn("failed to get cursor from {} to {}", fromJournalId + 1, newToJournalId);
            return false;
        }

        File currentEditFile = new File(snapshotDir, snapshotId);
        currentEditFile.createNewFile();
        EditLogFileOutputStream outputStream = new EditLogFileOutputStream(currentEditFile);

        boolean hasLog = false;
        while (true) {
            Pair<Long, JournalEntity> kv = cursor.next();
            if (kv == null) {
                break;
            }
            Long logId = kv.first;
            JournalEntity entity = kv.second;
            if (entity == null) {
                break;
            }
            hasLog = true;
            outputStream.write(entity.getOpCode(), entity.getData());
        }
        outputStream.setReadyToFlush();
        outputStream.flush();
        outputStream.close();

        return hasLog;
    }

    public void submitJob(CleanCopyJobTask task) {
        needScheduleJobs.add(task);
    }
}
