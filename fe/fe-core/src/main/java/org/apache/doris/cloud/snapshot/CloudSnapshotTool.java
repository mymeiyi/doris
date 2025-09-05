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

import org.apache.doris.cloud.catalog.CloudSnapshotEnv;
import org.apache.doris.cloud.storage.ListObjectsResult;
import org.apache.doris.cloud.storage.ObjectFile;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.common.DdlException;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.EditLogFileInputStream;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.meta.MetaReader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;

public class CloudSnapshotTool extends CloudSnapshotToolBase {

    private static final Logger LOG = LogManager.getLogger(CloudSnapshotTool.class);
    private String cloneSnapshotDir;

    // ==== for clone cluster snapshot ====

    @Override
    public void cloneClusterSnapshot(String clusterSnapshotFile, String metaDir) throws Exception {
        CloneSnapshotState cloneSnapshotState = parseClusterSnapshotFile(clusterSnapshotFile);
        createCloneSnapshotDir(metaDir);
        downloadImage(cloneSnapshotState);
        loadSnapshotImage();
        deleteCloneSnapshotDir();
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

    private void createCloneSnapshotDir(String metaDir) {
        this.cloneSnapshotDir = metaDir + "/clone-snapshot/";
        File cloneSnapshotDirFile = new File(this.cloneSnapshotDir);
        deleteCloneSnapshotDir();
        cloneSnapshotDirFile.mkdir();
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
            LOG.info("finished replay {} journal from cluster snapshot: {}, lastLogId: {}", count,
                    editLogFile.getAbsolutePath(), replayedJournalId + count);
        }

        // generate new image
        String latestImageFilePath = cloudSnapshotEnv.saveImage();
        replayedJournalId = cloudSnapshotEnv.getReplayedJournalId();
        LOG.info("save image to {}, replayedJournalId: {}", latestImageFilePath, replayedJournalId);
    }

    private void deleteCloneSnapshotDir() {
        File cloneSnapshotDirFile = new File(this.cloneSnapshotDir);
        if (cloneSnapshotDirFile.exists()) {
            if (cloneSnapshotDirFile.isDirectory()) {
                for (File f : cloneSnapshotDirFile.listFiles()) {
                    LOG.info("delete file: {}", f.getAbsolutePath());
                    f.delete();
                }
            }
            cloneSnapshotDirFile.delete();
            LOG.info("delete cloud snapshot directory: {}", cloneSnapshotDirFile.getAbsolutePath());
        }
    }
}
