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

import com.typesafe.config.Config
import io.snappydata.adanalytics.Configs._
import org.apache.spark.sql.streaming.{SchemaDStream, SnappyStreamingJob}
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation}
import org.apache.spark.streaming.SnappyStreamingContext

/**
  * Same as SnappySQLogAggregator except this streaming job runs in the data
  * store cluster. By implementing a SnappyStreamingJob we allow this program
  * to run managed in the snappy cluster.
  * Here we use Snappy SQL to process a stream as
  * micro-batches of DataFrames instead of using the Spark Streaming API based
  * on RDDs. This is similar to what we will see in Spark 2.0 (Structured
  * streaming).
  *
  * Run this program using bin/snappy-job.sh
  */
class SnappySQLLogAggregatorJob extends SnappyStreamingJob {

  override def runSnappyJob(snsc: SnappyStreamingContext, jobConfig: Config): Any = {
    //Spark tip : Keep shuffle count low when data volume is low.
    snsc.sql("set spark.sql.shuffle.partitions=8")

    snsc.sql("drop table if exists adImpressionStream")
    snsc.sql("drop table if exists sampledAdImpressions")
    snsc.sql("drop table if exists aggrAdImpressions")

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
      s" kafkaParams 'metadata.broker.list->$brokerList'," +
      s" topics '$kafkaTopic'," +
      " K 'java.lang.String'," +
      " V 'io.snappydata.adanalytics.AdImpressionLog', " +
      " KD 'kafka.serializer.StringDecoder', " +
      " VD 'io.snappydata.adanalytics.AdImpressionLogAvroDecoder')")

    // Next, create the Column table where we ingest all our data into.
    snsc.sql("create table aggrAdImpressions(time_stamp timestamp, publisher string," +
      " geo string, avg_bid double, imps long, uniques long) " +
      "using column options(buckets '11')")

    snsc.sql("CREATE SAMPLE TABLE sampledAdImpressions" +
      " OPTIONS(qcs 'geo,publisher', fraction '0.03', strataReservoirSize '50', baseTable 'aggrAdImpressions')")

    // Execute this query once every second. Output is a SchemaDStream.
    val resultStream: SchemaDStream = snsc.registerCQ(
      "select min(time_stamp), publisher, geo, avg(bid) as avg_bid," +
        " count(*) as imps , count(distinct(cookie)) as uniques" +
        " from adImpressionStream window (duration 1 seconds, slide 1 seconds)" +
        " where geo != 'unknown' group by publisher, geo")

    resultStream.foreachDataFrame(df => {
      df.write.insertInto("aggrAdImpressions")
      df.write.insertInto("sampledAdImpressions")
    })

    snsc.start()
    snsc.awaitTermination()
  }

  override def isValidJob(snsc: SnappyStreamingContext, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }
}
