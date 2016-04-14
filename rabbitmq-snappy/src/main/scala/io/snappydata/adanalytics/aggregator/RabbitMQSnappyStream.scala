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

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Duration, SnappyStreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * A Snappy streaming program to receive RabbitMQ messages
  */
object RabbitMQSnappyStream extends App {

  Logger.getRootLogger().setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("Benchmark")
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.executor.extraJavaOptions",
      " -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+AggressiveOpts -XX:FreqInlineSize=300 -XX:MaxInlineSize=300 ")
    .set("spark.streaming.blockInterval", "50")

  val sc = new SparkContext(sparkConf)
  val snsc = new SnappyStreamingContext(sc, Duration(1000))

  snsc.sql("drop table if exists adImpressions")
  snsc.sql("drop table if exists adImpressionStream")

  snsc.sql("create stream table adImpressionStream (" +
    " timestamp long," +
    " publisher string," +
    " advertiser string," +
    " website string," +
    " geo string," +
    " bid double," +
    " cookie string) " +
    " using rabbitmq_stream options" +
    " (storagelevel 'MEMORY_AND_DISK_SER_2'," +
    " rowConverter 'io.snappydata.adanalytics.aggregator.AdImpressionToRowsConverter' ," +
    " host 'localhost',"+
    " queueName 'rabbitmq-q',"+
    " T 'io.snappydata.adanalytics.aggregator.AdImpressionLog'," +
    " D 'io.snappydata.adanalytics.aggregator.RabbitMQAvroDecoder')")

  snsc.sql("create table adImpressions(timestamp long, publisher string, " +
    "advertiser string, website string, geo string, bid double, cookie string) " +
    "using column " +
    "options ( BUCKETS '29')")

  snsc.getSchemaDStream("adImpressionStream").foreachDataFrame(df => {
    df.show
    df.write.insertInto("adImpressions")
  })

  snsc.start()
  snsc.awaitTermination()
}
