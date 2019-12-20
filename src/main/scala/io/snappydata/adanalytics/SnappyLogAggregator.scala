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

package io.snappydata.adanalytics

import com.typesafe.config.{Config, ConfigFactory}
import io.snappydata.adanalytics.Configs._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{approx_count_distinct, avg, count, min, window}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Example using Spark API + Snappy extension to model a Stream as a DataFrame.
  *
  * This example can be run either in local mode or can be submitted as a job
  * to an already running SnappyData cluster.
  */
object SnappyLogAggregator extends SnappySQLJob with App {

  val conf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    .set("spark.ui.port", "4041")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .registerAvroSchemas(AdImpressionLog.getClassSchema)

  // add the "assembly" jar to executor classpath
  val assemblyJar = System.getenv("PROJECT_ASSEMBLY_JAR")
  if (assemblyJar != null) {
    conf.set("spark.driver.extraClassPath", assemblyJar)
    conf.set("spark.executor.extraClassPath", assemblyJar)
  }

  val sc = new SparkContext(conf)
  private val snappy = new SnappySession(sc)

  runSnappyJob(snappy, ConfigFactory.empty())

  /** Contains the implementation of the Job, Snappy uses this as
    * an entry point to execute Snappy job
    */
  override def runSnappyJob(snappy: SnappySession, jobConfig: Config): Any = {

    // The volumes are low. Optimize Spark shuffle by reducing the partition count
    snappy.sql("set spark.sql.shuffle.partitions=8")

    import org.apache.spark.sql.streaming.ProcessingTime
    snappy.sql("drop table if exists sampledAdImpressions")
    snappy.sql("drop table if exists aggrAdImpressions")

    snappy.sql("create table aggrAdImpressions(time_stamp timestamp, publisher string," +
      " geo string, avg_bid double, imps long, uniques long) " +
      "using column options(buckets '11')")
    snappy.sql("CREATE SAMPLE TABLE sampledAdImpressions" +
      " OPTIONS(qcs 'geo', fraction '0.03', strataReservoirSize '50', baseTable 'aggrAdImpressions')")

    val schema = StructType(Seq(StructField("timestamp", TimestampType), StructField("publisher", StringType),
      StructField("advertiser", StringType), StructField("website", StringType), StructField("geo", StringType),
      StructField("bid", DoubleType), StructField("cookie", StringType)))

    import snappy.implicits._
    val df = snappy.readStream
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
      .filter(s"geo != '${Configs.UnknownGeo}'")

    val windowedDF = df.withColumn("eventTime", $"timestamp".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "1 seconds", "1 seconds"), $"publisher", $"geo")
      .agg(min("timestamp").alias("timestamp"), avg("bid").alias("avg_bid"), count("geo").alias("imps"),
        approx_count_distinct("cookie").alias("uniques"))
      .select("timestamp", "publisher", "geo", "avg_bid", "imps", "uniques")

    val logStream = windowedDF
      .writeStream
      .format("snappysink")
      .queryName("log_aggregator")
      .trigger(ProcessingTime("1 seconds"))
      .option("tableName", "aggrAdImpressions")
      .option("checkpointLocation", snappyLogAggregatorCheckpointDir)
      .outputMode("update")
      .start

    logStream.awaitTermination()
  }

  override def isValidJob(snappy: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }
}