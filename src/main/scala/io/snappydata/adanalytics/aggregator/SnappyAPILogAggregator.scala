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

package io.snappydata.adanalytics.aggregator

import io.snappydata.adanalytics.aggregator
import Constants._
import kafka.serializer.StringDecoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, SnappyStreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SnappyAPILogAggregator extends App {

  val conf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    //.setMaster("snappydata://localhost:10334")
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .registerAvroSchemas(aggregator.AdImpressionLog.getClassSchema)

  val sc = new SparkContext(conf)
  val ssc = new SnappyStreamingContext(sc, batchDuration)

  // stream of (topic, ImpressionLog)
  val messages = KafkaUtils.createDirectStream
    [String, AdImpressionLog, StringDecoder, AdImpressionLogAvroDecoder](ssc, kafkaParams, topics)

  // Filter out bad messages ...use a 2 second window
  val logs = messages.map(_._2).filter(_.getGeo != Constants.UnknownGeo)
    .window(Duration.apply(2000), Duration.apply(2000))

  // Best to operate stream as a DataFrame/Table ... easy to run analytics on
  // stream
  val rows = logs.map(v => Row(v.getTimestamp, v.getPublisher, v
    .getAdvertiser, v.getWebsite, v.getGeo, v.getBid, v.getCookie))

  val logStreamAsTable = ssc.createSchemaDStream(rows, getAdImpressionSchema)

  import org.apache.spark.sql.functions._

  /**
    * We want to execute the following analytic query ... using the DataFrame
    * API ...
    * select publisher, geo, avg(bid) as avg_bid, count(*) imps, count(distinct(cookie)) uniques
    * from AdImpressionLog group by publisher, geo"
    */
  logStreamAsTable.foreachDataFrame(df => {
    val df1 = df.groupBy("publisher", "geo")
      .agg(avg("bid") as "avg_bid", count("geo") as "imps",
        countDistinct("cookie") as "uniques")
    df1.show()
  })

  // start rolling!
  ssc.start
  ssc.awaitTermination

  private def getAdImpressionSchema: StructType = {
    StructType(Array(
      StructField("timestamp", LongType, true),
      StructField("publisher", StringType, true),
      StructField("advertiser", StringType, true),
      StructField("website", StringType, true),
      StructField("geo", StringType, true),
      StructField("bid", DoubleType, true),
      StructField("cookie", StringType, true)))
  }
}