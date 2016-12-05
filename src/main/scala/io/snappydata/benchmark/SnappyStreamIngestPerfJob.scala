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

package io.snappydata.benchmark

import com.typesafe.config.Config
import io.snappydata.adanalytics.Configs._
import org.apache.spark.sql.streaming.SnappyStreamingJob
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.sql.{SnappyContext, SnappyJobValid, SnappyJobValidation, SnappySQLJob}

class SnappyStreamIngestPerfJob extends SnappyStreamingJob {

  override def runSnappyJob(snsc: SnappyStreamingContext, jobConfig: Config): Any = {
    //snsc.sql("drop table if exists adImpressions")
    snsc.sql("drop table if exists adImpressionStream")

    // Create a stream of AdImpressionLog which will pull the log messages
    // from Kafka broker
    snsc.sql("create stream table adImpressionStream (" +
      " time_stamp timestamp," +
      " publisher string," +
      " advertiser string," +
      " website string," +
      " geo string," +
      " bid double," +
      " cookie string) " +
      " using directkafka_stream options (" +
      " rowConverter 'io.snappydata.adanalytics.AdImpressionToRowsConverter' ," +
      s" kafkaParams 'metadata.broker.list->$brokerList'," +
      s" topics '$kafkaTopic'," +
      " K 'java.lang.String'," +
      " V 'io.snappydata.adanalytics.AdImpressionLog', " +
      " KD 'kafka.serializer.StringDecoder', " +
      " VD 'io.snappydata.adanalytics.AdImpressionLogAvroDecoder')")

    snsc.sql("create table adImpressions(times_tamp timestamp, publisher string, " +
      "advertiser string, website string, geo string, bid double, cookie string) " +
      "using column options ( buckets '29', persistent 'asynchronous')")

    snsc.sql("CREATE SAMPLE TABLE sampledAdImpressions" +
      " OPTIONS(qcs 'geo,publisher', fraction '0.02', strataReservoirSize '50', baseTable 'adImpressions')")

    // Save the streaming data to snappy store per second (btachDuration)
    snsc.getSchemaDStream("adImpressionStream").foreachDataFrame( df => {
        df.write.insertInto("adImpressions")
        df.write.insertInto("sampledAdImpressions")
      })

    snsc.start
    snsc.awaitTermination
  }

  override def isValidJob(snsc: SnappyStreamingContext, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }
}