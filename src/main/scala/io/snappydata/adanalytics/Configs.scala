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

  val numProducerThreads = 32

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
