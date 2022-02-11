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

import com.gemstone.gemfire.cache.execute.{FunctionAdapter, FunctionContext}
import com.gemstone.gemfire.cache.persistence.PersistentID
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.gemfire.extensions.FilteredBackupManager

class PrepareFilteredBackupFunction extends FunctionAdapter {

  override def getId: String = "PrepareFilteredBackup"

  override def isHA: Boolean = false

  def execute(includes: String, excludes: String): java.util.HashSet[PersistentID] = {
    val cache = GemFireCacheImpl.getInstance
    if (cache eq null) new java.util.HashSet[PersistentID]
    else {
      val manager = FilteredBackupManager.startBackup(cache, includes, excludes)
      manager.prepareBackup()
    }
  }

  override def execute(context: FunctionContext): Unit = {
    val args = context.getArguments.asInstanceOf[Array[String]]
    val result =
      if (args.length == 0) Boolean.box(cleanup()) else Misc.getMyId -> execute(args(0), args(1))
    context.getResultSender[AnyRef].lastResult(result)
  }

  def cleanup(): Boolean = {
    val cache = GemFireCacheImpl.getInstance
    if (cache ne null) cache.getBackupManager match {
      case null => false
      case b => b.asInstanceOf[FilteredBackupManager].cleanup(); true
    } else false
  }
}
