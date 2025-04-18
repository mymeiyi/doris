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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ClassLoaderUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.plugin.PropertiesUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AccessControllerManager is the entry point of privilege authentication.
 * There are 2 kinds of access controller:
 * SystemAccessController: for global level priv, resource priv and other Doris internal priv checking
 * CatalogAccessController: for specified catalog's priv checking, can be customized.
 * And using InternalCatalogAccessController as default.
 */
public class AccessControllerManager {
    private static final Logger LOG = LogManager.getLogger(AccessControllerManager.class);

    private Auth auth;
    // Default access controller instance used for handling cases where no specific controller is specified
    private CatalogAccessController defaultAccessController;
    // Map that stores the mapping between catalogs and their corresponding access controllers
    private Map<String, CatalogAccessController> ctlToCtlAccessController = Maps.newConcurrentMap();
    // Cache of loaded access controller factories for quick creation of new access controllers
    private ConcurrentHashMap<String, AccessControllerFactory> accessControllerFactoriesCache
            = new ConcurrentHashMap<>();
    // Mapping between access controller class names and their identifiers for easy lookup of factory identifiers
    private ConcurrentHashMap<String, String> accessControllerClassNameMapping = new ConcurrentHashMap<>();

    public AccessControllerManager(Auth auth) {
        this.auth = auth;
        loadAccessControllerPlugins();
        String accessControllerName = Config.access_controller_type;
        this.defaultAccessController = loadAccessControllerOrThrow(accessControllerName);
        ctlToCtlAccessController.put(InternalCatalog.INTERNAL_CATALOG_NAME, defaultAccessController);
    }

    private CatalogAccessController loadAccessControllerOrThrow(String accessControllerName) {
        if (accessControllerName.equalsIgnoreCase("default")) {
            return new InternalAccessController(auth);
        }
        if (accessControllerFactoriesCache.containsKey(accessControllerName)) {
            Map<String, String> prop;
            try {
                prop = PropertiesUtils.loadAccessControllerPropertiesOrNull();
            } catch (IOException e) {
                throw new RuntimeException("Failed to load authorization properties."
                        + "Please check the configuration file, authorization name is " + accessControllerName, e);
            }
            return accessControllerFactoriesCache.get(accessControllerName).createAccessController(prop);
        }
        throw new RuntimeException("No authorization plugin factory found for " + accessControllerName
                + ". Please confirm that your plugin is placed in the correct location.");
    }

    private void loadAccessControllerPlugins() {
        ServiceLoader<AccessControllerFactory> loaderFromClasspath = ServiceLoader.load(AccessControllerFactory.class);
        for (AccessControllerFactory factory : loaderFromClasspath) {
            LOG.info("Found Authentication Plugin Factories: {} from class path.", factory.factoryIdentifier());
            accessControllerFactoriesCache.put(factory.factoryIdentifier(), factory);
            accessControllerClassNameMapping.put(factory.getClass().getName(), factory.factoryIdentifier());
        }
        List<AccessControllerFactory> loader = null;
        try {
            loader = ClassLoaderUtils.loadServicesFromDirectory(AccessControllerFactory.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Authentication Plugin Factories", e);
        }
        for (AccessControllerFactory factory : loader) {
            LOG.info("Found Access Controller Plugin Factory: {} from directory.", factory.factoryIdentifier());
            accessControllerFactoriesCache.put(factory.factoryIdentifier(), factory);
            accessControllerClassNameMapping.put(factory.getClass().getName(), factory.factoryIdentifier());
        }
    }

    public CatalogAccessController getAccessControllerOrDefault(String ctl) {
        CatalogAccessController catalogAccessController = ctlToCtlAccessController.get(ctl);
        if (catalogAccessController != null) {
            return catalogAccessController;
        }
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(ctl);
        if (catalog != null && catalog instanceof ExternalCatalog) {
            lazyLoadCtlAccessController((ExternalCatalog) catalog);
            return ctlToCtlAccessController.get(ctl);
        }

        return defaultAccessController;
    }

    private synchronized void lazyLoadCtlAccessController(ExternalCatalog catalog) {
        if (ctlToCtlAccessController.containsKey(catalog.getName())) {
            return;
        }
        catalog.initAccessController(false);
        if (!ctlToCtlAccessController.containsKey(catalog.getName())) {
            ctlToCtlAccessController.put(catalog.getName(), defaultAccessController);
        }
    }

    public boolean checkIfAccessControllerExist(String ctl) {
        return ctlToCtlAccessController.containsKey(ctl);
    }

    public void createAccessController(String ctl, String acFactoryClassName, Map<String, String> prop,
                                       boolean isDryRun) {
        String pluginIdentifier = getPluginIdentifierForAccessController(acFactoryClassName);
        CatalogAccessController accessController = accessControllerFactoriesCache.get(pluginIdentifier)
                .createAccessController(prop);
        if (!isDryRun) {
            ctlToCtlAccessController.put(ctl, accessController);
            LOG.info("create access controller {} for catalog {}", acFactoryClassName, ctl);
        }
    }

    private String getPluginIdentifierForAccessController(String acClassName) {
        String pluginIdentifier = null;
        if (accessControllerClassNameMapping.containsKey(acClassName)) {
            pluginIdentifier = accessControllerClassNameMapping.get(acClassName);
        }
        if (accessControllerFactoriesCache.containsKey(acClassName)) {
            pluginIdentifier = acClassName;
        }
        if (null == pluginIdentifier || !accessControllerFactoriesCache.containsKey(pluginIdentifier)) {
            throw new RuntimeException("Access Controller Plugin Factory not found for " + acClassName);
        }
        return pluginIdentifier;
    }

    public void removeAccessController(String ctl) {
        if (StringUtils.isBlank(ctl)) {
            return;
        }
        if (ctlToCtlAccessController.containsKey(ctl)) {
            ctlToCtlAccessController.remove(ctl);
        }
        LOG.info("remove access controller for catalog {}", ctl);
    }

    public Auth getAuth() {
        return this.auth;
    }

    // ==== Global ====
    public boolean checkGlobalPriv(ConnectContext ctx, PrivPredicate wanted) {
        return checkGlobalPriv(ctx.getCurrentUserIdentity(), wanted);
    }

    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        return defaultAccessController.checkGlobalPriv(currentUser, wanted);
    }

    // ==== Catalog ====
    public boolean checkCtlPriv(ConnectContext ctx, String ctl, PrivPredicate wanted) {
        return checkCtlPriv(ctx.getCurrentUserIdentity(), ctl, wanted);
    }

    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        boolean hasGlobal = checkGlobalPriv(currentUser, wanted);
        // for checking catalog priv, always use InternalAccessController.
        // because catalog priv is only saved in InternalAccessController.
        return defaultAccessController.checkCtlPriv(hasGlobal, currentUser, ctl, wanted);
    }

    // ==== Database ====
    public boolean checkDbPriv(ConnectContext ctx, String ctl, String db, PrivPredicate wanted) {
        return checkDbPriv(ctx.getCurrentUserIdentity(), ctl, db, wanted);
    }

    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        boolean hasGlobal = checkGlobalPriv(currentUser, wanted);
        return getAccessControllerOrDefault(ctl).checkDbPriv(hasGlobal, currentUser, ctl, db, wanted);
    }

    // ==== Table ====
    public boolean checkTblPriv(ConnectContext ctx, TableName tableName, PrivPredicate wanted) {
        Preconditions.checkState(tableName.isFullyQualified());
        return checkTblPriv(ctx, tableName.getCtl(), tableName.getDb(), tableName.getTbl(), wanted);
    }

    public boolean checkTblPriv(ConnectContext ctx, TableNameInfo tableName, PrivPredicate wanted) {
        Preconditions.checkState(tableName.isFullyQualified());
        return checkTblPriv(ctx, tableName.getCtl(), tableName.getDb(), tableName.getTbl(), wanted);
    }

    public boolean checkTblPriv(ConnectContext ctx, String qualifiedCtl,
                                String qualifiedDb, String tbl, PrivPredicate wanted) {
        if (ctx.isSkipAuth()) {
            return true;
        }
        return checkTblPriv(ctx.getCurrentUserIdentity(), qualifiedCtl, qualifiedDb, tbl, wanted);
    }

    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        boolean hasGlobal = checkGlobalPriv(currentUser, wanted);
        return getAccessControllerOrDefault(ctl).checkTblPriv(hasGlobal, currentUser, ctl, db, tbl, wanted);
    }

    // ==== Column ====
    // If param has ctx, we can skip auth by isSkipAuth field in ctx
    public void checkColumnsPriv(ConnectContext ctx, String ctl, String qualifiedDb, String tbl, Set<String> cols,
                                 PrivPredicate wanted) throws UserException {
        if (ctx.isSkipAuth()) {
            return;
        }
        checkColumnsPriv(ctx.getCurrentUserIdentity(), ctl, qualifiedDb, tbl, cols, wanted);
    }

    public void checkColumnsPriv(UserIdentity currentUser, String
            ctl, String qualifiedDb, String tbl, Set<String> cols,
                                 PrivPredicate wanted) throws UserException {
        boolean hasGlobal = checkGlobalPriv(currentUser, wanted);
        CatalogAccessController accessController = getAccessControllerOrDefault(ctl);
        long start = System.currentTimeMillis();
        accessController.checkColsPriv(hasGlobal, currentUser, ctl, qualifiedDb,
                tbl, cols, wanted);
        if (LOG.isDebugEnabled()) {
            LOG.debug("checkColumnsPriv use {} mills, user: {}, ctl: {}, db: {}, table: {}, cols: {}",
                    System.currentTimeMillis() - start, currentUser, ctl, qualifiedDb, tbl, cols);
        }
    }

    // ==== Resource ====
    public boolean checkResourcePriv(ConnectContext ctx, String resourceName, PrivPredicate wanted) {
        return checkResourcePriv(ctx.getCurrentUserIdentity(), resourceName, wanted);
    }

    public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        return defaultAccessController.checkResourcePriv(currentUser, resourceName, wanted);
    }

    // ==== Cloud ====
    public boolean checkCloudPriv(ConnectContext ctx, String cloudName, PrivPredicate wanted, ResourceTypeEnum type) {
        return checkCloudPriv(ctx.getCurrentUserIdentity(), cloudName, wanted, type);
    }

    public boolean checkCloudPriv(UserIdentity currentUser, String cloudName,
                                  PrivPredicate wanted, ResourceTypeEnum type) {
        return defaultAccessController.checkCloudPriv(currentUser, cloudName, wanted, type);
    }

    public boolean checkStorageVaultPriv(ConnectContext ctx, String storageVaultName, PrivPredicate wanted) {
        return checkStorageVaultPriv(ctx.getCurrentUserIdentity(), storageVaultName, wanted);
    }

    public boolean checkStorageVaultPriv(UserIdentity currentUser, String storageVaultName, PrivPredicate wanted) {
        return defaultAccessController.checkStorageVaultPriv(currentUser, storageVaultName, wanted);
    }

    public boolean checkWorkloadGroupPriv(ConnectContext ctx, String workloadGroupName, PrivPredicate wanted) {
        return checkWorkloadGroupPriv(ctx.getCurrentUserIdentity(), workloadGroupName, wanted);
    }

    public boolean checkWorkloadGroupPriv(UserIdentity currentUser, String workloadGroupName, PrivPredicate wanted) {
        return defaultAccessController.checkWorkloadGroupPriv(currentUser, workloadGroupName, wanted);
    }

    // ==== Other ====
    public boolean checkPrivByAuthInfo(ConnectContext ctx, AuthorizationInfo authInfo, PrivPredicate wanted) {
        if (authInfo == null) {
            return false;
        }
        if (authInfo.getDbName() == null) {
            return false;
        }
        if (authInfo.getTableNameList() == null || authInfo.getTableNameList().isEmpty()) {
            return checkDbPriv(ctx, InternalCatalog.INTERNAL_CATALOG_NAME, authInfo.getDbName(), wanted);
        }
        for (String tblName : authInfo.getTableNameList()) {
            if (!checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, authInfo.getDbName(),
                    tblName, wanted)) {
                return false;
            }
        }
        return true;
    }

    public Map<String, Optional<DataMaskPolicy>> evalDataMaskPolicies(UserIdentity currentUser, String
            ctl, String db, String tbl, Set<String> cols) {
        Map<String, Optional<DataMaskPolicy>> res = Maps.newHashMap();
        for (String col : cols) {
            res.put(col, evalDataMaskPolicy(currentUser, ctl, db, tbl, col));
        }
        return res;
    }

    public Optional<DataMaskPolicy> evalDataMaskPolicy(UserIdentity currentUser, String
            ctl, String db, String tbl, String col) {
        Objects.requireNonNull(currentUser, "require currentUser object");
        Objects.requireNonNull(ctl, "require ctl object");
        Objects.requireNonNull(db, "require db object");
        Objects.requireNonNull(tbl, "require tbl object");
        Objects.requireNonNull(col, "require col object");
        return getAccessControllerOrDefault(ctl).evalDataMaskPolicy(currentUser, ctl, db, tbl, col.toLowerCase());
    }

    public List<? extends RowFilterPolicy> evalRowFilterPolicies(UserIdentity currentUser, String
            ctl, String db, String tbl) {
        Objects.requireNonNull(currentUser, "require currentUser object");
        Objects.requireNonNull(ctl, "require ctl object");
        Objects.requireNonNull(db, "require db object");
        Objects.requireNonNull(tbl, "require tbl object");
        return getAccessControllerOrDefault(ctl).evalRowFilterPolicies(currentUser, ctl, db, tbl);
    }
}
