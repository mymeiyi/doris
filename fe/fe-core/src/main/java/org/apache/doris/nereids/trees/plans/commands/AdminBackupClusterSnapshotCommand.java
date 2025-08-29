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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.load.CloudSnapshotHandler;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * ADMIN BACKUP CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = 'test_snapshot');
 */
public class AdminBackupClusterSnapshotCommand extends Command implements ForwardWithSync {

    public static final String PROP_TTL = "ttl";
    public static final String PROP_LABEL = "label";
    private static final long DEFAULT_EXPIRATION_MINUTES = 60;
    private static final Logger LOG = LogManager.getLogger(AdminBackupClusterSnapshotCommand.class);

    private Map<String, String> properties;
    private long ttl = -1;
    private String label = "";
    private long expirationMinutes = DEFAULT_EXPIRATION_MINUTES;    // default 60min

    /**
     * AdminBackupClusterSnapshotCommand
     */
    public AdminBackupClusterSnapshotCommand(Map<String, String> properties) {
        super(PlanType.ADMIN_BACKUP_CLUSTER_COMMAND);
        Objects.requireNonNull(properties, "properties is null");
        this.properties = properties;
    }

    public long getTtl() {
        return ttl;
    }

    public String getLabel() {
        return label;
    }

    public long getExpirationMinutes() {
        return expirationMinutes;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        LOG.info("sout: AdminBackupClusterSnapshotCommand is running");
        validate(ctx);
        CloudSnapshotHandler cloudSnapshotHandler = ((CloudEnv) ctx.getEnv()).getCloudSnapshotHandler();
        cloudSnapshotHandler.snapshot("1", "");
        // ctx.getEnv().getTabletChecker().cancelRepairTable(this);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (!Config.isCloudMode()) {
            throw new AnalysisException("The sql is illegal in disk mode ");
        }
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }

        if (properties == null) {
            return;
        }
        try {
            Iterator<Map.Entry<String, String>> iter = properties.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, String> entry = iter.next();
                if (entry.getKey().equalsIgnoreCase(PROP_TTL)) {
                    ttl = Long.valueOf(entry.getValue());
                    // iter.remove();
                } else if (entry.getKey().equalsIgnoreCase(PROP_LABEL)) {
                    label = entry.getValue();
                    // iter.remove();
                } else {
                    throw new AnalysisException("Unknown property: " + entry.getKey());
                }
            }
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid property: " + e.getMessage());
        }

        /*if (!properties.isEmpty()) {
            throw new AnalysisException("Unknown property: " + properties);
        }*/
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminBackupClusterSnapshotCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }
}
