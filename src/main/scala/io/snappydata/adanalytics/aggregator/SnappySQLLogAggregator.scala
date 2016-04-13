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

import Constants._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.SnappyStreamingContext

object SnappySQLLogAggregator extends App {

  val sparkConf = new org.apache.spark.SparkConf()
    .setAppName(getClass.getSimpleName)
    .set("spark.sql.inMemoryColumnarStorage.compressed", "false")
    .set("spark.sql.inMemoryColumnarStorage.batchSize", "2000")
    // .setMaster(s"spark://$hostName:7077") //split
    //.setMaster("local[*]") //local
    .setMaster("snappydata://localhost:10334")// embedded

  val sc = new SparkContext(sparkConf)
  val snsc = new SnappyStreamingContext(sc, batchDuration)

  snsc.sql("drop table if exists AdImpressionLog")
  snsc.sql("drop table if exists adImpressions")
  snsc.sql("drop table if exists sampledAdImpressions")

  snsc.sql("create stream table AdImpressionLog (" +
    " timestamp long," +
    " publisher string," +
    " advertiser string," +
    " website string," +
    " geo string," +
    " bid double," +
    " cookie string) " +
    " using directkafka_stream options" +
    " (storagelevel 'MEMORY_AND_DISK_SER_2'," +
    " rowConverter 'io.snappydata.adanalytics.aggregator.AdImpressionToRowsConverter' ," +
    s" kafkaParams '$brokerList'," +
    s" topics '$kafkaTopic'," +
    " K 'java.lang.String'," +
    " V 'io.snappydata.adanalytics.aggregator.AdImpressionLog', " +
    " KD 'kafka.serializer.StringDecoder', " +
    " VD 'io.snappydata.adanalytics.aggregator.AdImpressionLogAvroDecoder')")

  snsc.sql("create table adImpressions(timestamp long, publisher string, " +
    "advertiser string, website string, geo string, bid double, cookie string) " +
    "using column " +
    "options ( BUCKETS '29')")

  // insert streaming data directly into a column table
  snsc.getSchemaDStream("AdImpressionLog").foreachDataFrame(df => {
    df.write.insertInto("adImpressions")
  })

   snsc.sql("create table adImpressions(publisher string," +
    " geo string, avg_bid double, imps long, uniques long) " +
    "using column " +
    "options(PARTITION_BY 'publisher')")

//    snsc.sql("CREATE SAMPLE TABLE sampledAdImpressions (publisher string, geo string, avg_bid double, imps long, uniques long)" +
//    " OPTIONS(qcs 'publisher', fraction '0.03', strataReservoirSize '50')")

  snsc.registerCQ("select publisher, geo, avg(bid) as avg_bid, count(*) imps, count(distinct(cookie)) uniques" +
    " from AdImpressionLog window (duration '2' seconds, slide '2' seconds)" +
    " where geo != 'unknown' group by publisher, geo")
    .foreachDataFrame(df => {
      df.write.insertInto("adImpressions")
      //df.write.insertInto("sampledAdImpressions")
    })

  snsc.start
  snsc.awaitTermination
}
