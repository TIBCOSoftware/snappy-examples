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

import com.typesafe.config.Config
import io.snappydata.adanalytics.aggregator.Constants._
import kafka.serializer.StringDecoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.SnappyStreamingJob
import org.apache.spark.sql.types._
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.kafka.KafkaUtils
import spark.jobserver.{SparkJobValid, SparkJobValidation}

/**
 * Same as SnappyAPILogAggregator except this streaming job runs in the data
 * store cluster. By implementing a SnappyStreamingJob we allow this program
 * to run managed in the snappy cluster.
 * Here we use Snappy SQL extensions to process a stream as
 * micro-batches of DataFrames instead of using the Spark Streaming API based
 * on RDDs. This is similar to what we will see in Spark 2.0 (Structured
 * streaming).
 *
 * Run this program using bin/snappy-submit
 */
class SnappyAPILogAggregatorJob extends SnappyStreamingJob {

  override def runJob(snsc: C, jobConfig: Config): Any = {
    snsc.sql("set spark.sql.shuffle.partitions=8")
    // stream of (topic, ImpressionLog)
    val messages = KafkaUtils.createDirectStream
      [String, AdImpressionLog, StringDecoder, AdImpressionLogAvroDecoder](snsc, kafkaParams, topics)

    // Filter out bad messages ...use a 2 second window
    val logs = messages.map(_._2).filter(_.getGeo != Constants.UnknownGeo)
      .window(Duration.apply(2000), Duration.apply(2000))

    // Best to operate stream as a DataFrame/Table ... easy to run analytics on stream
    val rows = logs.map(v => Row(new java.sql.Timestamp(v.getTimestamp), v.getPublisher.toString,
      v.getAdvertiser.toString, v.getWebsite.toString, v.getGeo.toString, v.getBid, v.getCookie.toString))

    val logStreamAsTable = snsc.createSchemaDStream(rows, getAdImpressionSchema)

    import org.apache.spark.sql.functions._

    /**
      * We want to execute the following analytic query ... using the DataFrame
      * API ...
      * select publisher, geo, avg(bid) as avg_bid, count(*) imps, count(distinct(cookie)) uniques
      * from AdImpressionLog group by publisher, geo, timestamp"
      */
    logStreamAsTable.foreachDataFrame(df => {
      val df1 = df.groupBy("publisher", "geo", "timestamp")
        .agg(avg("bid").alias("avg_bid"), count("geo").alias("imps"),
          countDistinct("cookie").alias("uniques"))
      df1.show()
    })

    snsc.start()
    snsc.awaitTermination()
  }

  private def getAdImpressionSchema: StructType = {
    StructType(Array(
      StructField("timestamp", TimestampType, true),
      StructField("publisher", StringType, true),
      StructField("advertiser", StringType, true),
      StructField("website", StringType, true),
      StructField("geo", StringType, true),
      StructField("bid", DoubleType, true),
      StructField("cookie", StringType, true)))
  }

  override def validate(snsc: C, config: Config): SparkJobValidation = {
    SparkJobValid
  }
}