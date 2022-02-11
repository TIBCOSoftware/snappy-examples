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

package io.snappydata.extensions

import java.io.File
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.{Date, Set => JSet}

import scala.collection.JavaConverters._

import com.gemstone.gemfire.admin.internal.{AdminDistributedSystemImpl, FinishBackupRequest, FlushToDiskRequest}
import com.gemstone.gemfire.cache.persistence.PersistentID
import com.gemstone.gemfire.distributed.DistributedMember
import com.gemstone.gemfire.distributed.internal.{DM, InternalDistributedSystem}
import com.pivotal.gemfirexd.internal.engine.GfxdConstants
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdMessage
import com.pivotal.gemfirexd.internal.engine.sql.execute.FunctionUtils
import com.pivotal.gemfirexd.internal.engine.sql.execute.FunctionUtils.GetFunctionMembers
import com.typesafe.config.Config

import org.apache.spark.Logging
import org.apache.spark.sql._

object BackupDiskStore extends SnappySQLJob with GetFunctionMembers with Logging {

  private val backupDirConf = "backup.dir"
  private val backupBaselineConf = "backup.baseline"
  private val backupIncludesConf = "backup.includePattern"
  private val backupExcludesConf = "backup.excludePattern"

  private val allMembers = GfxdMessage.getOtherMembers

  override def runSnappyJob(session: SnappySession, config: Config): Any = {
    val backupDir = config.getString(backupDirConf)
    // Baseline directory should be null if it was not provided on the command line
    val baselineDir =
      if (config.hasPath(backupBaselineConf)) new File(config.getString(backupBaselineConf))
      else null
    val includes =
      if (config.hasPath(backupIncludesConf)) config.getString(backupIncludesConf) else ".*"
    val excludes =
      if (config.hasPath(backupExcludesConf)) config.getString(backupExcludesConf) else ""

    // data dictionary cannot be excluded
    val excludePattern = Pattern.compile(excludes, Pattern.CASE_INSENSITIVE)
    if (excludePattern.matcher(GfxdConstants.GFXD_DD_DISKSTORE_NAME).matches()) {
      throw new IllegalArgumentException(
        s"Data dictionary cannot be excluded from backup (exclude pattern = $excludes)")
    }
    val format = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    val targetDir = new File(backupDir, format.format(new Date()))
    val (successful, missing) = backupAllMembers(
      InternalDistributedSystem.getConnectedInstance.getDistributionManager,
      targetDir, baselineDir, includes, excludes)

    val message = new StringBuilder
    message.append("The following disk stores were backed up (target directory '")
        .append(targetDir.getAbsolutePath).append("'):\n")
    for (memberStores <- successful) {
      memberStores.asScala.foreach(store => message.append('\t').append(store).append('\n'))
    }
    if (!missing.isEmpty) {
      message ++= "The backup may be incomplete. The following disk stores are not online:\n"
      missing.asScala.foreach(store => message.append('\t').append(store))
    }
    else message ++= "Backup successful."

    message.append('\n').toString()
  }

  def backupAllMembers(dm: DM, targetDir: File, baselineDir: File, includes: String,
      excludes: String): (Iterable[JSet[PersistentID]], JSet[PersistentID]) = {
    val missingMembers = AdminDistributedSystemImpl.getMissingPersistentMembers(dm)
    FlushToDiskRequest.send(dm, allMembers)
    try {
      val prepareBackup = new PrepareFilteredBackupFunction
      val existingDataStoreList = FunctionUtils.onMembers(dm.getSystem, this, true)
          .withArgs(Array(includes, excludes)).execute(prepareBackup)
          .getResult.asInstanceOf[java.util.List[(DistributedMember, JSet[PersistentID])]]
      // add self to backup list for disk store meta files
      existingDataStoreList.add(dm.getId -> prepareBackup.execute(includes, excludes))
      var existingDataStores = existingDataStoreList.asScala.toMap
      // backup the data dictionary first (if included)
      val successfulMembers1 = FinishBackupRequest.send(dm, allMembers, targetDir,
        baselineDir, FinishBackupRequest.DISKSTORE_DD)
      // backup all required disk stores other than data dictionary
      val successfulMembers = FinishBackupRequest.send(dm, allMembers, targetDir,
        baselineDir, FinishBackupRequest.DISKSTORE_ALL_BUT_DD)
      // create the full combined map
      successfulMembers1.asScala.foreach(p => successfulMembers.get(p._1) match {
        case null => successfulMembers.put(p._1, p._2)
        case ids => ids.addAll(p._2)
      })
      // It's possible that when calling getMissingPersistentMembers, some members are
      // still creating/recovering regions, and at FinishBackupRequest.send, the
      // regions at the members are ready. Logically, since the members in successfulMembers
      // should override the previous missingMembers
      for (onlineMembersIds <- successfulMembers.values().asScala) {
        missingMembers.removeAll(onlineMembersIds)
      }
      existingDataStores --= successfulMembers.keySet().asScala
      for (lostMembersIds <- existingDataStores.values) {
        missingMembers.addAll(lostMembersIds)
      }
      (successfulMembers.values().asScala, missingMembers)
    } finally {
      val prepareBackup = new PrepareFilteredBackupFunction
      FunctionUtils.onMembers(dm.getSystem, this, false)
          .withArgs(Array.empty[String]).execute(prepareBackup).getResult
      prepareBackup.cleanup()
    }
  }

  override def isValidJob(session: SnappySession, config: Config): SnappyJobValidation = {
    try {
      if (config.getString(backupDirConf).isEmpty) SnappyJobInvalid(s"empty $backupDirConf")
      else SnappyJobValid()
    } catch {
      case e: Exception => SnappyJobInvalid(s"No backup directory specified: $e")
    }
  }

  override def getMembers: JSet[DistributedMember] = allMembers

  override def getServerGroups: JSet[String] = null

  override def postExecutionCallback(): Unit = {}
}
