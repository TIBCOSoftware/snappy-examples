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

import java.io.FileReader

import com.opencsv.CSVReader
import io.snappydata.adanalytics.aggregator.AdImpressionLog
import io.snappydata.adanalytics.aggregator.Configs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Queue
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object CSVSnappyIngestionPerf extends App {

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

  val rddQueue = Queue[RDD[AdImpressionLog]]()

  val logStream = snsc.queueStream(rddQueue)

  val rows = logStream.map(v => Row(new java.sql.Timestamp(v.getTimestamp), v.getPublisher.toString,
    v.getAdvertiser.toString, v.getWebsite.toString, v.getGeo.toString, v.getBid, v.getCookie.toString))

  val logStreamAsTable = snsc.createSchemaDStream(rows, getAdImpressionSchema)

  snsc.snappyContext.createTable("adImpressions", "column", getAdImpressionSchema,
    Map("buckets" -> "29"))

  logStreamAsTable.foreachDataFrame(_.write.insertInto("adImpressions"))

  val csvReader = Future {
    import collection.JavaConverters._

    val csvFile = new CSVReader(new FileReader("adimpressions.csv"))
    csvFile.iterator.asScala
      .map { fields => {
        val log = new AdImpressionLog()
        log.setTimestamp(fields(0).toLong)
        log.setPublisher(fields(1))
        log.setAdvertiser(fields(2))
        log.setWebsite(fields(3))
        log.setGeo(fields(4))
        log.setBid(fields(5).toDouble)
        log.setCookie(fields(6))
        log
      }
      }.grouped(100000).foreach { logs =>
      val logRDD = sc.parallelize(logs, 8)
      rddQueue += logRDD
    }
  }

  csvReader.onComplete {
    case Success(value) =>
    case Failure(e) => e.printStackTrace
  }

  snsc.start()
  snsc.awaitTermination()

}
