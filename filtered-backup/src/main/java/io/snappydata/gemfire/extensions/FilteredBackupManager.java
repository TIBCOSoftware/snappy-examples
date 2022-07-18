/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package io.snappydata.gemfire.extensions;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.admin.internal.FinishBackupRequest;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.JarClassLoader;
import com.gemstone.gemfire.internal.JarDeployer;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.persistence.BackupInspector;
import com.gemstone.gemfire.internal.cache.persistence.BackupManager;
import com.gemstone.gemfire.internal.cache.persistence.RestoreScript;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilteredBackupManager extends BackupManager {

  static final Logger logger = LoggerFactory.getLogger(FilteredBackupManager.class);

  private final GemFireCacheImpl gfCache;
  private final Pattern includePattern;
  private final Pattern excludePattern;

  private final CountDownLatch filteredAllowDestroys = new CountDownLatch(1);
  private volatile boolean filteredIsCancelled = false;
  private volatile RestoreScript incompleteFilteredRestoreScript;

  public FilteredBackupManager(GemFireCacheImpl cache, String includes, String excludes) {
    super(Misc.getMyId(), cache);
    this.gfCache = cache;
    this.includePattern = Pattern.compile(includes, Pattern.CASE_INSENSITIVE);
    this.excludePattern = Pattern.compile(excludes, Pattern.CASE_INSENSITIVE);
  }

  public static FilteredBackupManager startBackup(GemFireCacheImpl cache,
      String includes, String excludes) throws Exception {
    FilteredBackupManager manager = new FilteredBackupManager(cache, includes, excludes);
    // set the backup manager into GemFireCacheImpl using reflection
    Field backupManagerField = GemFireCacheImpl.class.getDeclaredField("backupManager");
    backupManagerField.setAccessible(true);
    @SuppressWarnings("unchecked")
    AtomicReference<BackupManager> backupManager =
        (AtomicReference<BackupManager>)backupManagerField.get(cache);
    if (!backupManager.compareAndSet(null, manager)) {
      throw new IOException("Backup already in progress");
    }
    logger.info("Set the backup manager as " + cache.getBackupManager());
    manager.start();
    return manager;
  }

  @Override
  public void start() {
    // skips check for sender which will always be the lead
  }

  protected Stream<DiskStoreImpl> listAllDiskStoresToBackup() {
    return gfCache.listDiskStoresIncludingRegionOwned().stream().filter(store ->
        includePattern.matcher(store.getName()).matches() &&
            !excludePattern.matcher(store.getName()).matches());
  }

  public void cleanup() {
    filteredIsCancelled = true;
    if (filteredAllowDestroys.getCount() > 0) {
      filteredAllowDestroys.countDown();
    }
    this.incompleteFilteredRestoreScript = null;
    listAllDiskStoresToBackup().forEach(DiskStoreImpl::releaseBackupLock);
    final DM distributionManager = gfCache.getDistributedSystem().getDistributionManager();
    distributionManager.removeAllMembershipListener(this);
    gfCache.clearBackupManager();
  }

  @Override
  public HashSet<PersistentID> prepareBackup() {
    final HashSet<PersistentID> persistentIds = new HashSet<>();
    listAllDiskStoresToBackup().forEach(store -> {
      store.lockStoreBeforeBackup();
      if (store.hasPersistedData()) {
        persistentIds.add(store.getPersistentID());
        store.getStats().startBackup();
      }
    });
    return persistentIds;
  }

  /**
   * Returns the memberId directory for this member in the baseline. The memberId may have changed
   * if this member has been restarted since the last backup.
   *
   * @param baselineParentDir parent directory of last backup.
   *
   * @return null if the baseline for this member could not be located.
   */
  private File findBaselineForThisMember(File baselineParentDir) {
    /*
     * Find the first matching DiskStoreId directory for this member.
     */
    return listAllDiskStoresToBackup().map(store -> {
      File baselineDir = FileUtil.find(baselineParentDir, ".*" + store.getBackupDirName() + "$");
      /*
       * We found it? Good. Set this member's baseline to the backed up disk store's
       * member dir (two levels up).
       */
      return baselineDir != null ? baselineDir.getParentFile().getParentFile() : null;
    }).filter(Objects::nonNull).findFirst().orElse(null);
  }

  /**
   * Performs a sanity check on the baseline directory for incremental backups. If a baseline
   * directory exists for the member and there is no INCOMPLETE_BACKUP file then return the data
   * stores directory for this member.
   *
   * @param baselineParentDir a previous backup directory. This is used with the incremental
   *                          backup option.  May be null if the user specified a full backup.
   *
   * @return null if the backup is to be a full backup otherwise return the data store directory
   * in the previous backup for this member (if incremental).
   */
  private File checkBaseline(File baselineParentDir) {
    File baselineDir = null;

    if (null != baselineParentDir) {
      // Start by looking for this memberId
      baselineDir = getBackupDir(baselineParentDir);

      if (!baselineDir.exists()) {
        // hmmm, did this member have a restart?
        // Determine which member dir might be a match for us
        baselineDir = findBaselineForThisMember(baselineParentDir);
      }

      if (null != baselineDir) {
        // check for existence of INCOMPLETE_BACKUP file
        File incompleteBackup = new File(baselineDir, INCOMPLETE_BACKUP);
        if (incompleteBackup.exists()) {
          baselineDir = null;
        }
      }
    }

    return baselineDir;
  }

  private Stream<DiskStoreImpl> getDiskStoresToBackup(byte diskStoresToBackup) {
    switch (diskStoresToBackup) {
      case FinishBackupRequest.DISKSTORE_ALL:
        return listAllDiskStoresToBackup();

      case FinishBackupRequest.DISKSTORE_ALL_BUT_DD:
        return listAllDiskStoresToBackup()
            .filter(ds -> !ds.getName().equals(GfxdConstants.GFXD_DD_DISKSTORE_NAME));

      case FinishBackupRequest.DISKSTORE_DD:
        return listAllDiskStoresToBackup()
            .filter(ds -> ds.getName().equals(GfxdConstants.GFXD_DD_DISKSTORE_NAME));

      default:
        throw new IllegalArgumentException(
            "getDiskStoresToBackUp: unknown argument = " + diskStoresToBackup);
    }
  }

  @Override
  public HashSet<PersistentID> finishBackup(File targetDir,
      File baselineDir, byte diskStoresToBackup) throws IOException {
    try {
      File backupDir = getBackupDir(targetDir);

      // Make sure our baseline is okay for this member
      baselineDir = checkBaseline(baselineDir);

      // Create an inspector for the baseline backup
      BackupInspector inspector =
          (baselineDir == null ? null : BackupInspector.createInspector(baselineDir));

      File storesDir = new File(backupDir, DATA_STORES);
      RestoreScript restoreScript = this.incompleteFilteredRestoreScript == null
          ? new RestoreScript() : this.incompleteFilteredRestoreScript;
      HashSet<PersistentID> persistentIds = new HashSet<>();
      // convert stream to list since need to traverse it two times
      List<DiskStoreImpl> diskStores = getDiskStoresToBackup(diskStoresToBackup)
          .collect(Collectors.toList());

      // if include pattern skipped data dictionary then INCOMPLETE_BACKUP file will not exist
      boolean doCreateDir = diskStoresToBackup == FinishBackupRequest.DISKSTORE_ALL_BUT_DD &&
          !includePattern.matcher(GfxdConstants.GFXD_DD_DISKSTORE_NAME).matches();
      boolean foundPersistentData = false;
      for (Iterator<DiskStoreImpl> itr = diskStores.iterator(); itr.hasNext(); ) {
        DiskStoreImpl store = itr.next();
        if (store.hasPersistedData()) {
          if (!foundPersistentData &&
              (diskStoresToBackup == FinishBackupRequest.DISKSTORE_ALL ||
                  diskStoresToBackup == FinishBackupRequest.DISKSTORE_DD || doCreateDir)) {
            createBackupDir(backupDir);
          }
          if (!foundPersistentData) {
            foundPersistentData = true;
          }
          File diskStoreDir = new File(storesDir, store.getBackupDirName());
          // noinspection ResultOfMethodCallIgnored
          diskStoreDir.mkdir();
          store.startBackup(diskStoreDir, inspector, restoreScript);
        } else {
          itr.remove();
        }
        store.releaseBackupLock();
      }

      if (diskStoresToBackup == FinishBackupRequest.DISKSTORE_ALL_BUT_DD
          || diskStoresToBackup == FinishBackupRequest.DISKSTORE_ALL) {
        filteredAllowDestroys.countDown();
        this.incompleteFilteredRestoreScript = null;
      }

      for (DiskStoreImpl store : diskStores) {
        store.finishBackup(this);
        store.getStats().endBackup();
        persistentIds.add(store.getPersistentID());
      }

      if (foundPersistentData && (diskStoresToBackup == FinishBackupRequest.DISKSTORE_ALL
          || diskStoresToBackup == FinishBackupRequest.DISKSTORE_ALL_BUT_DD)) {
        backupConfigFiles(backupDir);
        backupUserFiles(restoreScript, backupDir);
        backupDeployedJars(restoreScript, backupDir);
        restoreScript.generate(backupDir);
        File incompleteFile = new File(backupDir, INCOMPLETE_BACKUP);
        if (!incompleteFile.delete()) {
          throw new IOException("Could not delete file " + INCOMPLETE_BACKUP);
        }
      }

      if (diskStoresToBackup == FinishBackupRequest.DISKSTORE_DD) {
        incompleteFilteredRestoreScript = restoreScript;
      }
      return persistentIds;

    } finally {
      if (diskStoresToBackup == FinishBackupRequest.DISKSTORE_ALL_BUT_DD
          || diskStoresToBackup == FinishBackupRequest.DISKSTORE_ALL) {
        cleanup();
      }
    }
  }

  private void backupConfigFiles(File backupDir) throws IOException {
    File configBackupDir = new File(backupDir, CONFIG);
    FileUtil.mkdirs(configBackupDir);
    URL url = gfCache.getCacheXmlURL();
    if (url != null) {
      File cacheXMLBackup = new File(configBackupDir,
          DistributionConfig.DEFAULT_CACHE_XML_FILE.getName());
      FileUtil.copy(url, cacheXMLBackup);
    }

    URL propertyURL = DistributedSystem.getPropertyFileURL();
    if (propertyURL != null) {
      File propertyBackup = new File(configBackupDir, "gemfire.properties");
      FileUtil.copy(propertyURL, propertyBackup);
    }
  }

  private void backupUserFiles(RestoreScript restoreScript, File backupDir) throws IOException {
    List<File> backupFiles = gfCache.getBackupFiles();
    File userBackupDir = new File(backupDir, USER_FILES);
    if (!userBackupDir.exists()) {
      // noinspection ResultOfMethodCallIgnored
      userBackupDir.mkdir();
    }
    for (File original : backupFiles) {
      if (original.exists()) {
        original = original.getAbsoluteFile();
        File dest = new File(userBackupDir, original.getName());
        FileUtil.copy(original, dest);
        restoreScript.addExistenceTest(original);
        restoreScript.addFile(original, dest);
      }
    }
  }

  /**
   * Copies user deployed jars to the backup directory.
   *
   * @param restoreScript Used to restore from this backup.
   * @param backupDir     The backup directory for this member.
   *
   * @throws IOException one or more of the jars did not successfully copy.
   */
  private void backupDeployedJars(RestoreScript restoreScript, File backupDir) throws IOException {
    JarDeployer deployer = null;

    try {
      deployer = new JarDeployer();

      /*
       * Suspend any user deployed jar file updates during this
       * backup.
       */
      deployer.suspendAll();

      List<JarClassLoader> jarList = deployer.findJarClassLoaders();
      if (!jarList.isEmpty()) {
        File userBackupDir = new File(backupDir, USER_FILES);
        if (!userBackupDir.exists()) {
          // noinspection ResultOfMethodCallIgnored
          userBackupDir.mkdir();
        }

        for (JarClassLoader loader : jarList) {
          File source = new File(loader.getFileCanonicalPath());
          File dest = new File(userBackupDir, source.getName());
          FileUtil.copy(source, dest);
          restoreScript.addFile(source, dest);
        }
      }
    } finally {
      /*
       * Re-enable user deployed jar file updates.
       */
      if (null != deployer) {
        deployer.resumeAll();
      }
    }
  }

  private File getBackupDir(File targetDir) {
    InternalDistributedMember memberId = gfCache.getDistributedSystem().getDistributedMember();
    String vmId = memberId.toString();
    vmId = cleanSpecialCharacters(vmId);
    return new File(targetDir, vmId);
  }

  private void createBackupDir(File backupDir) throws IOException {
    if (backupDir.exists()) {
      throw new IOException("Backup directory " + backupDir.getAbsolutePath() + " already exists.");
    }

    if (!FileUtil.mkdirs(backupDir)) {
      throw new IOException("Could not create directory: " + backupDir);
    }

    File incompleteFile = new File(backupDir, INCOMPLETE_BACKUP);
    if (!incompleteFile.createNewFile()) {
      throw new IOException("Could not create file: " + incompleteFile);
    }

    File readme = new File(backupDir, README);
    try (FileOutputStream fos = new FileOutputStream(readme)) {
      String text = LocalizedStrings.BackupManager_README.toLocalizedString();
      fos.write(text.getBytes());
    }
  }

  private String cleanSpecialCharacters(String string) {
    return string.replaceAll("[^\\w]+", "_");
  }

  @Override
  public void memberDeparted(InternalDistributedMember id, boolean crashed) {
    cleanup();
  }

  @Override
  public void waitForBackup() {
    try {
      filteredAllowDestroys.await();
    } catch (InterruptedException e) {
      throw new InternalGemFireError(e);
    }
  }

  @Override
  public boolean isCancelled() {
    return filteredIsCancelled;
  }
}
