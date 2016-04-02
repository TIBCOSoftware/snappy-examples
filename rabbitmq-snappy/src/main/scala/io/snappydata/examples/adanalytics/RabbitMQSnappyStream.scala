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

import com.stratio.receiver.RabbitMQUtils
import io.snappydata.examples.adanalytics.Constants._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.streaming.SnappyStreamingContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * A Snappy streaming program to receive RabbitMQ messages
  */
object RabbitMQSnappyStream extends App {

  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.registerAvroSchemas(io.snappydata.examples.adanalytics.AdImpressionLog.getClassSchema)
  val sc = SnappyContext.getOrCreate(new SparkContext(conf))
  val snsc = SnappyStreamingContext(sc, batchDuration)
  Logger.getRootLogger().setLevel(Level.ERROR)

  snsc.sql("drop table if exists adImpressions")

  // Setup the receiver stream to connect to RabbitMQ.
  val rabStream = RabbitMQUtils.createStream[AdImpressionLog](snsc, Map(
    "host" -> "localhost",
    "queueName" -> "rabbitmq-q"
  ))

  // val df =
    snsc.sql("create table adImpressions(timestamp long, publisher string, " +
    "advertiser string, website string, geo string, bid double, cookie string) " +
    "using column " +
    "options ( BUCKETS '29')")

  // val schema = df.schema
  val schema = StructType(
      StructField("timestamp", LongType, false) ::
      StructField("publisher", StringType, false) ::
      StructField("advertiser", StringType, false) ::
      StructField("website", StringType, false) ::
      StructField("geo", StringType, false) ::
      StructField("bid", DoubleType, false) ::
      StructField("cookie", StringType, false) :: Nil)

  val rowConverter = new ImpressionLogToRow

  rabStream.foreachRDD(rdd => {
    rdd.foreach(log => {
      println("Received AdImpressionLog - " + log)
    })
    /*val adImpressionsDF = sc.createDataFrame(rowConverter.toRowRDD(rdd), schema)
    adImpressionsDF.write.insertInto("adImpressions")
    adImpressionsDF.show(5)*/
  })
  snsc.start
  snsc.awaitTermination
}
