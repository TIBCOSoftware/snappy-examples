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

package io.snappydata.benchmark

import io.snappydata.adanalytics.{Configs, AvroSocketStreamConverter}
import Configs._
import io.snappydata.adanalytics.AdImpressionLog
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Simple Snappy streaming program which pulls log messages
  * from socket and ingest those log messages to Snappy store.
  */
object SocketSnappyIngestionPerf extends App {

  val sparkConf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    .setMaster(s"$sparkMasterURL")
    //.setMaster("snappydata://localhost:10334")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.executor.extraJavaOptions",
      " -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+AggressiveOpts -XX:FreqInlineSize=300 -XX:MaxInlineSize=300 ")
    .set("spark.streaming.blockInterval", "50")

  val assemblyJar = System.getenv("PROJECT_ASSEMBLY_JAR")
  if (assemblyJar != null) {
    sparkConf.set("spark.driver.extraClassPath", assemblyJar)
    sparkConf.set("spark.executor.extraClassPath", assemblyJar)
  }

  val sc = new SparkContext(sparkConf)

  val snsc = new SnappyStreamingContext(sc, batchDuration)

  snsc.snappyContext.dropTable("adImpressions", ifExists = true)

  val converter = new AvroSocketStreamConverter

  val logStream = snsc.socketStream[AdImpressionLog](hostname, socketPort, converter.convert, StorageLevel.MEMORY_ONLY)

  val rows = logStream.map(v => Row(new java.sql.Timestamp(v.getTimestamp), v.getPublisher.toString,
    v.getAdvertiser.toString, v.getWebsite.toString, v.getGeo.toString, v.getBid, v.getCookie.toString))

  val logStreamAsTable = snsc.createSchemaDStream(rows, getAdImpressionSchema)

  snsc.snappyContext.createTable("adImpressions", "column", getAdImpressionSchema,
    Map("buckets" -> "29"))

  logStreamAsTable.foreachDataFrame(_.write.insertInto("adImpressions"))

  snsc.start()
  snsc.awaitTermination()
}
