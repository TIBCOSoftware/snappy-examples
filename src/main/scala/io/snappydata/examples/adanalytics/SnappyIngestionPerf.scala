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

package io.snappydata.examples.adanalytics

import io.snappydata.examples.adanalytics.Constants._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.streaming.SnappyStreamingContext


/**
  * Simple direct kafka spark streaming program which pulls log messages
  * from kafka broker and ingest those log messages to Snappy store.
  */
object SnappyIngestionPerf extends App {

  val sparkConf = new org.apache.spark.SparkConf()
    .setAppName(getClass.getSimpleName)
    .set("spark.sql.inMemoryColumnarStorage.compressed", "false")
    .set("spark.sql.inMemoryColumnarStorage.batchSize", "2000")
    .setMaster("local[*]")

  val sc = new SparkContext(sparkConf)
  // batchDuration of 1 second
  val snsc = SnappyStreamingContext(SnappyContext.getOrCreate(sc), batchDuration)

  snsc.sql("drop table if exists adImpressionStream")
  snsc.sql("drop table if exists adImpressions")

  // Create a stream of AdImpressionLog which will pull the log messages
  // from Kafka broker
  snsc.sql("create stream table adImpressionStream (" +
    " timestamp long," +
    " publisher string," +
    " advertiser string," +
    " website string," +
    " geo string," +
    " bid double," +
    " cookie string) " +
    " using directkafka_stream options (" +
    " storagelevel 'MEMORY_AND_DISK_SER_2'," +
    " rowConverter 'io.snappydata.examples.adanalytics.KafkaStreamToRowsConverter' ," +
    " kafkaParams 'metadata.broker.list->localhost:9092'," +
    " topics 'adlogsTopic'," +
    " K 'java.lang.String'," +
    " V 'io.snappydata.examples.adanalytics.AdImpressionLog', " +
    " KD 'kafka.serializer.StringDecoder', " +
    " VD 'io.snappydata.examples.adanalytics.AdImpressionLogAvroDecoder')")

  // create a columnar table in Snappy store
  snsc.sql("create table adImpressions(timestamp long, publisher string, " +
    "advertiser string, website string, geo string, bid double, cookie string) " +
    "using column " +
    "options ( BUCKETS '29')")

  // Save the streaming data to snappy store per second (btachDuration)
  snsc.getSchemaDStream("adImpressionStream").foreachDataFrame(df => {
    df.write.insertInto("adImpressions")
  })

  snsc.start
  snsc.awaitTermination
}
