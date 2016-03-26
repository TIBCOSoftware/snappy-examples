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


import com.twitter.algebird.HyperLogLogMonoid
import kafka.serializer.StringDecoder
import org.apache.commons.io.Charsets
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import io.snappydata.examples.adanalytics.Constants._

object SnappySparkLogAggregator extends App {

  val sc = new SparkConf()
    .setAppName("SnappySparkLogAggregator")
    .setMaster("snappydata://localhost:10334")
  val ssc = new StreamingContext(sc, Seconds(1))

  // stream of (topic, ImpressionLog)
  val messages = KafkaUtils.createDirectStream
    [String, AdImpressionLog, StringDecoder, AdImpressionLogAvroDecoder](ssc, kafkaParams, topics)

  // to count uniques
  lazy val hyperLogLog = new HyperLogLogMonoid(12)

  // we filter out non resolved geo (unknown) and map (pub, geo) -> AggLog that will be reduced
  val logsByPubGeo = messages.map(_._2).filter(_.getGeo != Constants.UnknownGeo).map {
    log =>
      val key = PublisherGeoKey(log.getPublisher.toString, log.getGeo.toString)
      val agg = AggregationLog(
        timestamp = log.getTimestamp,
        sumBids = log.getBid,
        imps = 1,
        uniquesHll = hyperLogLog(log.getCookie.toString.getBytes(Charsets.UTF_8))
      )
      (key, agg)
  }

  // Reduce to generate imps, uniques, sumBid per pub and geo per 2 seconds
  val aggLogs = logsByPubGeo.reduceByKeyAndWindow(reduceAggregationLogs, Seconds(2))

  aggLogs.foreachRDD(rdd => println(rdd.count))

  // start rolling!
  ssc.start
  ssc.awaitTermination
  private def reduceAggregationLogs(aggLog1: AggregationLog, aggLog2: AggregationLog) = {
    aggLog1.copy(
      timestamp = math.min(aggLog1.timestamp, aggLog2.timestamp),
      sumBids = aggLog1.sumBids + aggLog2.sumBids,
      imps = aggLog1.imps + aggLog2.imps,
      uniquesHll = aggLog1.uniquesHll + aggLog2.uniquesHll
    )
  }
}