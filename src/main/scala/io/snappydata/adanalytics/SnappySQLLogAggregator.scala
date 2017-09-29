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

import io.snappydata.adanalytics.Configs._
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.SchemaDStream
import org.apache.spark.streaming.SnappyStreamingContext

/**
 * We use Snappy SQL extensions to process a stream as
 * micro-batches of DataFrames instead of using the Spark Streaming API based
 * on RDDs. This is similar to what we will see in Spark 2.0 (Structured
 * streaming).
 *
 * Not only does the use of SQL permit optimizations in the spark engine but
 * we make the Stream visible as a Table to external clients. For instance,
 * you can connect using JDBC and run a query on the stream table.
 *
 * This program will run in a standalong JVM and connect to the Snappy
 * cluster as the data store.
 */
object SnappySQLLogAggregator extends App {

  val sparkConf = new org.apache.spark.SparkConf()
    .setAppName(getClass.getSimpleName)
    .set("spark.sql.inMemoryColumnarStorage.compressed", "false")
    .set("spark.sql.inMemoryColumnarStorage.batchSize", "2000")
    .setMaster(s"$sparkMasterURL")
    .set("snappydata.store.locators", s"$snappyLocators")
    .set("spark.ui.port", "4041")
    .set("spark.streaming.kafka.maxRatePerPartition", s"$maxRatePerPartition")

  // add the "assembly" jar to executor classpath
  val assemblyJar = System.getenv("PROJECT_ASSEMBLY_JAR")
  if (assemblyJar != null) {
    sparkConf.set("spark.driver.extraClassPath", assemblyJar)
    sparkConf.set("spark.executor.extraClassPath", assemblyJar)
  }

  val sc = new SparkContext(sparkConf)
  val snsc = new SnappyStreamingContext(sc, batchDuration)

  //Spark tip : Keep shuffle count low when data volume is low.
  snsc.sql("set spark.sql.shuffle.partitions=8")

  snsc.sql("drop table if exists aggrAdImpressions")
  snsc.sql("drop table if exists sampledAdImpressions")
  snsc.sql("drop table if exists adImpressionStream")

  /**
   * Create a stream over the Kafka source. The messages are converted to Row
   * objects and comply with the schema defined in the 'create' below.
   * This is mostly just a SQL veneer over Spark Streaming. The stream table
   * is also automatically registered with the SnappyData catalog so external
   * clients can see this stream as a table
   */
  snsc.sql("create stream table adImpressionStream (" +
    " time_stamp timestamp," +
    " publisher string," +
    " advertiser string," +
    " website string," +
    " geo string," +
    " bid double," +
    " cookie string) " +
    " using directkafka_stream options(" +
    " rowConverter 'io.snappydata.adanalytics.AdImpressionToRowsConverter' ," +
    s" kafkaParams 'metadata.broker.list->$brokerList;auto.offset.reset->smallest'," +
    s" topics '$kafkaTopic'," +
    " K 'java.lang.String'," +
    " V 'io.snappydata.adanalytics.AdImpressionLog', " +
    " KD 'kafka.serializer.StringDecoder', " +
    " VD 'io.snappydata.adanalytics.AdImpressionLogAvroDecoder')")

  // Next, create the Column table where we ingest all our data into.
   snsc.sql("create table aggrAdImpressions(time_stamp timestamp, publisher string," +
    " geo string, avg_bid double, imps long, uniques long) " +
     "using column options(buckets '11')")
  // You can make these tables persistent, add partitioned keys, replicate
  // for HA, overflow to HDFS, etc, etc. ... Read the docs.

  snsc.sql("CREATE SAMPLE TABLE sampledAdImpressions" +
    " OPTIONS(qcs 'geo', fraction '0.03', strataReservoirSize '50', baseTable 'aggrAdImpressions')")

  // Execute this query once every second. Output is a SchemaDStream.
  val resultStream : SchemaDStream = snsc.registerCQ(
    "select time_stamp, publisher, geo, avg(bid) as avg_bid," +
    " count(*) as imps , count(distinct(cookie)) as uniques" +
    " from adImpressionStream window (duration 1 seconds, slide 1 seconds)"+
    " where geo != 'unknown' group by publisher, geo, time_stamp")

  resultStream.foreachDataFrame( df => {
    df.write.insertInto("aggrAdImpressions")
  })
  // Above we use the Spark Data Source API to write to our Column table.
  // This will automatically localize the partitions in the data store. No
  // Shuffling.

  snsc.start()
  snsc.awaitTermination()
}
