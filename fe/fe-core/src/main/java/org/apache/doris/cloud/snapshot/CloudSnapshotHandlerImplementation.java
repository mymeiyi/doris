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

package org.apache.doris.cloud.snapshot;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudSnapshotEnv;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.storage.ListObjectsResult;
import org.apache.doris.cloud.storage.ObjectFile;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.master.Checkpoint;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.EditLogFileInputStream;
import org.apache.doris.persist.EditLogFileOutputStream;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.Storage;
import org.apache.doris.persist.meta.MetaReader;
import org.apache.doris.rpc.RpcException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class CloudSnapshotHandlerImplementation extends CloudSnapshotHandler {

    private static final Logger LOG = LogManager.getLogger(CloudSnapshotHandlerImplementation.class);

    private static final String SNAPSHOT_DIR = "/snapshot/";
    private static final String CLONE_SNAPSHOT_DIR = "/clone-snapshot/";
    private String snapshotDir;
    private String cloneSnapshotDir;

    // auto snapshot job
    private CloudSnapshotJob autoSnapshotJob = null;
    private long autoSnapshotIntervalSeconds;
    private long lastFinishedAutoSnapshotTime = -1; // second
    private boolean autoSnapshotJobInitialized = false;

    // manual snapshot jobs
    private LinkedBlockingQueue<CloudSnapshotJob> manualSnapshotJobs = Queues.newLinkedBlockingQueue();

    @Override
    public void initialize() {
        this.snapshotDir = Config.meta_dir + SNAPSHOT_DIR;
        createDir(this.snapshotDir);
        if (!Config.ak.isEmpty()) {
            lastFinishedAutoSnapshotTime = 0;
            autoSnapshotIntervalSeconds = 3600;
            autoSnapshotJob = new CloudSnapshotJob(true);
            autoSnapshotJobInitialized = true;
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            getLastFinishedAutoSnapshotTime();
            if (!autoSnapshotJobInitialized) {
                refreshAutoSnapshotJob();
            }
            executeJobs();
        } catch (Throwable e) {
            LOG.warn("failed to process one round of cloud snapshot", e);
        }
    }

    @Override
    public void submitJob(CloudSnapshotJob job) {
        manualSnapshotJobs.add(job);
    }

    @Override
    public synchronized void refreshAutoSnapshotJob() {
        Cloud.GetInstanceResponse response = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudInstance();
        Cloud.SnapshotSwitchStatus switchStatus = response.getInstance().getSnapshotSwitchStatus();
        if (switchStatus == Cloud.SnapshotSwitchStatus.SNAPSHOT_SWITCH_ON
                && response.getInstance().getMaxReservedSnapshot() > 0) {
            if (this.autoSnapshotJob == null) {
                this.autoSnapshotJob = new CloudSnapshotJob(true);
            }
            this.autoSnapshotIntervalSeconds = response.getInstance().getSnapshotIntervalSeconds();
        } else {
            this.autoSnapshotJob = null;
        }
        autoSnapshotJobInitialized = true;
    }

    private void getLastFinishedAutoSnapshotTime() {
        if (lastFinishedAutoSnapshotTime >= 0) {
            return;
        }
        try {
            Cloud.ListSnapshotResponse response = listSnapshot(false);
            for (Cloud.SnapshotInfoPB snapshotInfoPB : response.getSnapshotsList()) {
                if (!snapshotInfoPB.getAutoSnapshot()) {
                    continue;
                }
                if (snapshotInfoPB.getFinishAt() > lastFinishedAutoSnapshotTime) {
                    lastFinishedAutoSnapshotTime = snapshotInfoPB.getFinishAt();
                }
            }
            if (lastFinishedAutoSnapshotTime == -1) {
                lastFinishedAutoSnapshotTime = 0;
            }
            LOG.info("lastFinishedAutoSnapshotTime: {}", lastFinishedAutoSnapshotTime);
        } catch (DdlException e) {
            LOG.warn("failed to list snapshot", e);
        }
    }

    private void executeJobs() {
        while (true) {
            if (manualSnapshotJobs.isEmpty()) {
                break;
            }
            CloudSnapshotJob job = manualSnapshotJobs.poll();
            try {
                executeJob(job);
            } catch (Exception e) {
                LOG.warn("manual snapshot job failed: {}", job, e);
            }
        }
        if (autoSnapshotJob != null && lastFinishedAutoSnapshotTime + autoSnapshotIntervalSeconds
                < System.currentTimeMillis() / 1000) {
            try {
                String label = "auto_snapshot_" + System.currentTimeMillis();
                autoSnapshotJob.setLabel(label);
                executeJob(autoSnapshotJob);
            } catch (Exception e) {
                LOG.warn("auto snapshot job failed: {}", autoSnapshotJob, e);
            } finally {
                autoSnapshotJob.setLabel(null);
            }
        }
    }

    private void executeJob(CloudSnapshotJob job) {
        String snapshotId = null;
        long logId = 0;
        try {
            // 0. begin snapshot
            String imageUrl = null;
            Cloud.ObjectStoreInfoPB objInfo;
            synchronized (Env.getCurrentEnv().getEditLog()) {
                Cloud.BeginSnapshotResponse response = beginSnapshot(job);
                snapshotId = response.getSnapshotId();
                imageUrl = response.getImageUrl();
                objInfo = response.getObjInfo();
                // 1. write edit log
                SnapshotState snapshotState = new SnapshotState(snapshotId, imageUrl);
                logId = Env.getCurrentEnv().getEditLog().logBeginSnapshot(snapshotState);
            }
            // 2. upload image
            Checkpoint checkpoint = Env.getCurrentEnv().getCheckpointer();
            checkpoint.getLock().readLock().lock();
            try {
                uploadImage(snapshotId, imageUrl, objInfo, logId);
            } finally {
                checkpoint.getLock().readLock().unlock();
            }
            // 3. commit snapshot
            commitSnapshot(snapshotId, imageUrl, logId);
            if (job.isAuto()) {
                lastFinishedAutoSnapshotTime = System.currentTimeMillis() / 1000;
            }
            LOG.info("succeed to snapshot for job: {}, id: {}, imageUrl: {}, logId: {}", job, snapshotId, imageUrl,
                    logId);
        } catch (Exception e) {
            LOG.warn("failed to snapshot for job: {}", job, e);
            // abort snapshot
            try {
                if (snapshotId != null) {
                    abortSnapshot(snapshotId, e.getMessage());
                }
            } catch (Exception e1) {
                LOG.warn("failed to abort snapshot for job: {}", job, e1);
            }
            // delete edit log file
            try {
                File editLogFile = getEditLogFile(logId);
                if (editLogFile.exists()) {
                    editLogFile.delete();
                    LOG.info("delete edit log file: {}", editLogFile.getAbsolutePath());
                }
            } catch (Exception e1) {
                LOG.warn("failed to delete edit log file for job: {}", job, e1);
            }
        }
    }

    private Cloud.BeginSnapshotResponse beginSnapshot(CloudSnapshotJob job) throws Exception {
        if (!Config.ak.isEmpty()) {
            Cloud.ObjectStoreInfoPB objectStoreInfoPB = Cloud.ObjectStoreInfoPB.newBuilder()
                    .setProvider(Cloud.ObjectStoreInfoPB.Provider.COS).setEndpoint("cos.ap-beijing.myqcloud.com")
                    .setRegion("ap-beijing").setBucket(Config.bucket).setPrefix("meiyi").setAk(Config.ak)
                    .setSk(Config.sk).build();
            long timestamp = System.currentTimeMillis();
            Cloud.BeginSnapshotResponse response = Cloud.BeginSnapshotResponse.newBuilder()
                    .setSnapshotId("test-snapshot-id-" + timestamp)
                    .setImageUrl(objectStoreInfoPB.getPrefix() + "/snapshot/test-image-url-" + timestamp)
                    .setObjInfo(objectStoreInfoPB).build();
            return response;
        }
        Cloud.BeginSnapshotRequest.Builder builder = Cloud.BeginSnapshotRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id).setTimeoutSeconds(Config.cloud_snapshot_timeout_seconds)
                .setAutoSnapshot(job.isAuto());
        if (job.getTtl() > 0) {
            builder.setTtlSeconds(job.getTtl());
        }
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

    private void commitSnapshot(String snapshotId, String imageUrl, long logId) throws Exception {
        if (!Config.ak.isEmpty()) {
            return;
        }
        try {
            Cloud.CommitSnapshotRequest request = Cloud.CommitSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id).setSnapshotId(snapshotId).setImageUrl(imageUrl)
                    .setLastJournalId(logId).build();
            Cloud.CommitSnapshotResponse response = MetaServiceProxy.getInstance().commitSnapshot(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("commitSnapshot response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }

    private void uploadImage(String snapshotId, String imageUrl, Cloud.ObjectStoreInfoPB objInfo,
            long logId) throws Exception {
        LOG.info("start to snapshot for id: {}, imageUrl: {}, logId: {}", snapshotId, imageUrl, logId);
        // 1. upload image file
        long imageVersion = getImageVersion();
        if (imageVersion > logId) {
            throw new DdlException("image version " + imageVersion + " is larger than log id " + logId);
        }
        // TODO use lock to prevent checkpoint
        RemoteBase remote = RemoteBase.newInstance(new RemoteBase.ObjectInfo(objInfo));
        if (imageVersion > 0) {
            String imageDir = Env.getServingEnv().getImageDir();
            String imageFileName = "image." + imageVersion;
            File imageFile = new File(imageDir + "/" + imageFileName);
            if (!imageFile.exists()) {
                LOG.error("image file does not exist: {}", imageFile.getAbsoluteFile());
                throw new DdlException("image file does not exist: " + imageFile.getAbsoluteFile());
            }
            remote.putObject(imageFile, imageUrl + "/" + imageFileName);
        }
        // 2. scan edit logs between imageVersion + 1 and logId, upload edit log
        if (imageVersion + 1 < logId) {
            writeSnapshotEditLogFile(imageVersion + 1, logId, snapshotId);
            File snapshotEditLogFile = getEditLogFile(logId);
            remote.putObject(snapshotEditLogFile, imageUrl + "/" + snapshotEditLogFile.getName());
            snapshotEditLogFile.delete();
        }
    }

    private File getEditLogFile(long logId) {
        return new File(this.snapshotDir, "edits." + logId);
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

    private void writeSnapshotEditLogFile(long fromJournalId, long toJournalId, String snapshotId) throws Exception {
        LOG.info("scan journal from {} to {} for snapshotId: {}", fromJournalId, toJournalId, snapshotId);
        JournalCursor cursor = Env.getCurrentEnv().getEditLog().getJournal().read(fromJournalId, toJournalId, false);
        if (cursor == null) {
            LOG.warn("failed to get cursor from {} to {}", fromJournalId, toJournalId);
            throw new DdlException("failed to get cursor from " + fromJournalId + " to " + toJournalId);
        }

        File snapshotEditLogFile = new File(this.snapshotDir, "edits." + toJournalId);
        if (snapshotEditLogFile.exists()) {
            snapshotEditLogFile.delete();
        }
        if (!snapshotEditLogFile.createNewFile()) {
            LOG.warn("failed to create snapshot edits log file {}", snapshotEditLogFile.getAbsolutePath());
            throw new Exception("failed to create snapshot edits log file " + snapshotEditLogFile.getAbsolutePath());
        }
        EditLogFileOutputStream outputStream = null;
        try {
            outputStream = new EditLogFileOutputStream(snapshotEditLogFile);
            while (true) {
                Pair<Long, JournalEntity> kv = cursor.next();
                if (kv == null) {
                    break;
                }
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
            LOG.warn("write snapshot edit log failed for id: {}", snapshotId, e);
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException ex) {
                    LOG.warn("failed to close output stream for id: {}", snapshotId, ex);
                }
            }
            try {
                if (snapshotEditLogFile.exists()) {
                    snapshotEditLogFile.delete();
                }
            } catch (Exception ex) {
                LOG.warn("failed to delete snapshot file for id: {}", snapshotId, ex);
            }
            throw new Exception(e.getMessage());
        }
    }

    private void abortSnapshot(String snapshotId, String reason) throws Exception {
        try {
            Cloud.AbortSnapshotRequest request = Cloud.AbortSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id).setSnapshotId(snapshotId).setReason(reason).build();
            Cloud.AbortSnapshotResponse response = MetaServiceProxy.getInstance().abortSnapshot(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("abortSnapshot response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }

    // ==== for clone cluster snapshot ====

    @Override
    public void cloneSnapshot(String clusterSnapshotFile) throws Exception {
        CloneSnapshotState cloneSnapshotState = parseClusterSnapshotFile(clusterSnapshotFile);
        this.cloneSnapshotDir = Config.meta_dir + CLONE_SNAPSHOT_DIR;
        createDir(this.cloneSnapshotDir);
        downloadImage(cloneSnapshotState);
        loadSnapshotImage();
        truncateDir(this.cloneSnapshotDir);
    }

    private CloneSnapshotState parseClusterSnapshotFile(String clusterSnapshotFile) {
        LOG.info("load cluster snapshot from file: {}", clusterSnapshotFile);
        File file = new File(clusterSnapshotFile);
        if (!file.exists()) {
            LOG.error("cluster snapshot file {} does not exist", clusterSnapshotFile);
            System.exit(-1);
        }

        CloneSnapshotState cloneSnapshotState = null;
        try {
            cloneSnapshotState = new ObjectMapper().readValue(file, CloneSnapshotState.class);
        } catch (Exception e) {
            LOG.error("failed to parse cluster snapshot file {}", clusterSnapshotFile, e);
            System.exit(-1);
        }
        return cloneSnapshotState;
    }

    private void downloadImage(CloneSnapshotState cloneSnapshotState) throws Exception {
        String fromSnapshotId = cloneSnapshotState.getFromSnapshotId();
        RemoteBase.ObjectInfo objectInfo = cloneSnapshotState.getObjInfo();
        LOG.info("start to download snapshot id: {}", fromSnapshotId);
        RemoteBase remote = RemoteBase.newInstance(objectInfo);
        // TODO fix
        String key = "snapshot/" + fromSnapshotId + "/";
        ListObjectsResult listObjectsResult = remote.listObjects(key, null);
        for (ObjectFile objectFile : listObjectsResult.getObjectInfoList()) {
            String lastPart = objectFile.getKey().substring(objectFile.getKey().lastIndexOf("/") + 1);
            String localPath = cloneSnapshotDir + lastPart;
            LOG.info("download objectFile: {}  to local path: {}", objectFile.toString(), localPath);
            remote.getObject(objectFile.getKey(), localPath);
        }
    }

    private void loadSnapshotImage() throws IOException, DdlException {
        File dir = new File(this.cloneSnapshotDir);
        File[] files = dir.listFiles();
        if (files.length == 0 || files.length > 2) {
            LOG.error("clone snapshot directory: {} contains {} files", dir.getAbsolutePath(), files.length);
            System.exit(-1);
        }
        File imageFile = null;
        File editLogFile = null;
        for (File file : dir.listFiles()) {
            if (file.getName().startsWith("image.")) {
                imageFile = file;
            } else {
                editLogFile = file;
            }
        }

        CloudSnapshotEnv cloudSnapshotEnv = new CloudSnapshotEnv(false);
        // load image
        long replayedJournalId = 0;
        if (imageFile != null) {
            String fileName = imageFile.getName();
            replayedJournalId = Long.parseLong(fileName.substring(fileName.lastIndexOf(".") + 1));
            MetaReader.read(imageFile, cloudSnapshotEnv);
            LOG.info("finished load image from cluster snapshot: {}, replayedJournalId: {}",
                    imageFile.getAbsolutePath(), replayedJournalId);
        }

        // replay edit log
        if (editLogFile != null) {
            long count = 0;
            DataInputStream currentStream = new DataInputStream(
                    new BufferedInputStream(new EditLogFileInputStream(editLogFile)));
            try {
                while (true) {
                    JournalEntity entity = new JournalEntity();
                    entity.readFields(currentStream);
                    count++;
                    if (entity.getOpCode() == OperationType.OP_LOCAL_EOF) {
                        break;
                    }
                    EditLog.loadJournal(cloudSnapshotEnv, replayedJournalId + count, entity);
                    replayedJournalId++;
                }
            } catch (IOException e) {
                try {
                    currentStream.close();
                } catch (IOException e1) {
                    LOG.error("failed to close cluster snapshot edit log", e1);
                }
                if (!(e instanceof EOFException)) {
                    LOG.error("failed to replay cluster snapshot edit log", e);
                    System.exit(-1);
                }
            }
            LOG.info("finished replay {} journal from cluster snapshot: {}, lastLogId: {}, replayedJournalId: {}",
                    count, editLogFile.getAbsolutePath(), replayedJournalId + count, replayedJournalId);
        }

        // generate new image
        cloudSnapshotEnv.setReplayedJournalId(replayedJournalId);
        String latestImageFilePath = cloudSnapshotEnv.saveImage();
        LOG.info("save image to {}, replayedJournalId: {}", latestImageFilePath, replayedJournalId);
    }

    private void createDir(String dir) {
        truncateDir(dir);
        File directory = new File(dir);
        if (!directory.mkdir()) {
            LOG.error("failed to create directory: {}", directory.getAbsolutePath());
        }
    }

    private void truncateDir(String dir) {
        File directory = new File(dir);
        if (directory.exists()) {
            if (directory.isDirectory()) {
                for (File file : directory.listFiles()) {
                    if (!file.delete()) {
                        LOG.warn("failed to delete file: {}", file.getAbsolutePath());
                    } else {
                        LOG.info("delete file: {}", file.getAbsolutePath());
                    }
                }
            }
            if (!directory.delete()) {
                LOG.warn("failed to delete directory: {}", directory.getAbsolutePath());
            } else {
                LOG.info("delete directory: {}", directory.getAbsolutePath());
            }
        }
    }
}
