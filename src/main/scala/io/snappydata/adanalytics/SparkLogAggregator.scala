/*
 * Copyright (c) 2016-2020 TIBCO Software Inc. All rights reserved.
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

package io.snappydata.adanalytics

import io.snappydata.adanalytics.Configs._
import org.apache.commons.lang.StringEscapeUtils
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Vanilla Spark implementation with no Snappy extensions being used. The aggregated data is
 * written to a kafka topic.
 *
 * Following command should be used to submit this job:
 *
 * {{{
 * ./bin/spark-submit --class io.snappydata.adanalytics.SparkLogAggregator \
 * --master <spark-master-url> <path to snappy poc assembly jar>
 * }}}
 */
object SparkLogAggregator extends App {

  val sc = new SparkConf()
    .setAppName(getClass.getName)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val ssc = new StreamingContext(sc, Seconds(1))
  val schema = StructType(Seq(StructField("timestamp", TimestampType), StructField("publisher", StringType),
    StructField("advertiser", StringType), StructField("website", StringType), StructField("geo", StringType),
    StructField("bid", DoubleType), StructField("cookie", StringType)))

  private val spark = SparkSession.builder().getOrCreate()

  import spark.implicits._

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokerList)
    .option("value.deserializer", classOf[ByteArrayDeserializer].getName)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 100000)
    .option("subscribe", kafkaTopic)
    .load().select("value").as[Array[Byte]](Encoders.BINARY)
    .mapPartitions(itr => {
      val deserializer = new AdImpressionLogAVRODeserializer
      itr.map(data => {
        val adImpressionLog = deserializer.deserialize(data)
        Row(new java.sql.Timestamp(adImpressionLog.getTimestamp), adImpressionLog.getPublisher.toString,
          adImpressionLog.getAdvertiser.toString, adImpressionLog.getWebsite.toString,
          adImpressionLog.getGeo.toString, adImpressionLog.getBid, adImpressionLog.getCookie.toString)
      })
    })(RowEncoder.apply(schema))
    .filter(s"geo != '${Configs.UnknownGeo}'")   // filtering invalid data

  // Group by on sliding window of 1 second
  val windowedDF = df.withColumn("eventTime", $"timestamp".cast("timestamp"))
    .withWatermark("eventTime", "10 seconds")
    .groupBy(window($"eventTime", "1 seconds", "1 seconds"), $"publisher", $"geo")
    .agg(min("timestamp").alias("timestamp"), avg("bid").alias("avg_bid"), count("geo").alias
    ("imps"), approx_count_distinct("cookie").alias("uniques"))
    .select("timestamp", "publisher", "geo", "avg_bid", "imps", "uniques")

  // content of 'value' column will be written to kafka topic as value
  private val targetSchema = StructType(Seq(StructField("value", StringType)))
  implicit val encoder: ExpressionEncoder[Row] = RowEncoder(targetSchema)

  // writing aggregated records on a kafka topic in CSV format
  val logStream = windowedDF
    .map(r => Row(r.toSeq.map(f => StringEscapeUtils.escapeCsv(f.toString)).mkString(",")))
    .writeStream
    .queryName("spark_log_aggregator")
    .option("checkpointLocation", sparkLogAggregatorCheckpointDir)
    .outputMode("update")
    .format("kafka")
    .option("kafka.bootstrap.servers", brokerList)
    .option("topic", "adImpressionsOut")
    .start()

  logStream.awaitTermination()
}
