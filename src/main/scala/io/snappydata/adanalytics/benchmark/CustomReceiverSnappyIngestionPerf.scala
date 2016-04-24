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

package io.snappydata.adanalytics.benchmark

import io.snappydata.adanalytics.aggregator.AdImpressionLog
import io.snappydata.adanalytics.aggregator.Constants._
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Duration, SnappyStreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object CustomReceiverSnappyIngestionPerf extends App {

  val sparkConf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val assemblyJar = System.getenv("PROJECT_ASSEMBLY_JAR")
  if (assemblyJar != null) {
    sparkConf.set("spark.driver.extraClassPath", assemblyJar)
    sparkConf.set("spark.executor.extraClassPath", assemblyJar)
  }

  val sc = new SparkContext(sparkConf)
  val snsc = new SnappyStreamingContext(sc, Duration(1000))

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
      store(generateAdImpression())
    }
  }

  private def generateAdImpression(): AdImpressionLog = {
    val numPublishers = 50
    val numAdvertisers = 30
    val publishers = (0 to numPublishers).map("publisher" +)
    val advertisers = (0 to numAdvertisers).map("advertiser" +)
    val unknownGeo = "un"

    val geos = Seq("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL",
      "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
      "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM",
      "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
      "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", unknownGeo)

    val numWebsites = 999
    val numCookies = 999
    val websites = (0 to numWebsites).map("website" +)
    val cookies = (0 to numCookies).map("cookie" +)

    val random = new java.util.Random()
    val timestamp = System.currentTimeMillis()
    val publisher = publishers(random.nextInt(numPublishers - 10 + 1) + 10)
    val advertiser = advertisers(random.nextInt(numAdvertisers - 10 + 1) + 10)
    val website = websites(random.nextInt(numWebsites - 100 + 1) + 100)
    val cookie = cookies(random.nextInt(numCookies - 100 + 1) + 100)
    val geo = geos(random.nextInt(geos.size))
    val bid = math.abs(random.nextDouble()) % 1

    val log = new AdImpressionLog()
    log.setTimestamp(timestamp)
    log.setPublisher(publisher)
    log.setAdvertiser(advertiser)
    log.setWebsite(website)
    log.setGeo(geo)
    log.setBid(bid)
    log.setCookie(cookie)
    log
  }
}