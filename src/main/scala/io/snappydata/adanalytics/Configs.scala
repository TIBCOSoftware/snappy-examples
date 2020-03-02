/*
* Copyright © 2019. TIBCO Software Inc.
* This file is subject to the license terms contained
* in the license file that is distributed with this file.
*/

package io.snappydata.adanalytics

object Configs {

  val kafkaTopic = "adImpressionsTopic"

  val brokerList = "localhost:9092"

  val kafkaParams: Map[String, String] = Map(
    "metadata.broker.list" -> brokerList
  )

  // Ideally checkpoint directory should be at some shared HDFS location accessible by all the nodes
  val snappyLogAggregatorCheckpointDir = s"/tmp/snappyLogAggregator"
  val sparkLogAggregatorCheckpointDir = s"/tmp/sparkLogAggregator"

  val hostname = "localhost"

  val socketPort = 9000

  val numPublishers = 50

  val numAdvertisers = 30

  val publishers = (0 to numPublishers).map("publisher" +)

  val advertisers = (0 to numAdvertisers).map("advertiser" +)

  val numProducerThreads = 1

  val UnknownGeo = "un"

  val geos: Seq[String] = Seq("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM",
    "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
    "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", UnknownGeo)

  val numGeos: Int = geos.size

  val numWebsites = 999

  val numCookies = 999

  val websites = (0 to numWebsites).map("website" +)

  val cookies = (0 to numCookies).map("cookie" +)

  val numLogsPerThread = 20000000

  val maxLogsPerSecPerThread = 5000
}
