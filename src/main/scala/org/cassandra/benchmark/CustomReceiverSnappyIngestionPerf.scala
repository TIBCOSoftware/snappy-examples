/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

package org.cassandra.benchmark

import io.snappydata.adanalytics.{Configs, AdImpressionGenerator}
import Configs._
import io.snappydata.adanalytics.AdImpressionLog
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.{SparkConf, SparkContext}

object CustomReceiverSnappyIngestionPerf extends App {

  val sparkConf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    .setMaster(s"$sparkMasterURL")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val assemblyJar = System.getenv("PROJECT_ASSEMBLY_JAR")
  if (assemblyJar != null) {
    sparkConf.set("spark.driver.extraClassPath", assemblyJar)
    sparkConf.set("spark.executor.extraClassPath", assemblyJar)
  }

  val sc = new SparkContext(sparkConf)
  val snsc = new SnappyStreamingContext(sc, batchDuration)

  snsc.snappyContext.dropTable("adImpressions", ifExists = true)

  val stream = snsc.receiverStream[AdImpressionLog](new AdImpressionReceiver)

  val rows = stream.map(v => Row(new java.sql.Timestamp(v.getTimestamp), v.getPublisher.toString,
    v.getAdvertiser.toString, v.getWebsite.toString, v.getGeo.toString, v.getBid, v.getCookie.toString))

  val logStreamAsTable = snsc.createSchemaDStream(rows, getAdImpressionSchema)

  snsc.snappyContext.createTable("adImpressions", "column", getAdImpressionSchema,
    Map("buckets" -> "29"))

  logStreamAsTable.foreachDataFrame(_.write.insertInto("adImpressions"))

  snsc.start()
  snsc.awaitTermination()

}


final class AdImpressionReceiver extends Receiver[AdImpressionLog](StorageLevel.MEMORY_AND_DISK_2) {
  override def onStart() {
    new Thread("AdImpressionReceiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  override def onStop() {
  }

  private def receive() {
    while (!isStopped()) {
      store(AdImpressionGenerator.nextRandomAdImpression())
    }
  }
}