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

import org.apache.spark.streaming.Seconds

object Constants {

  val kafkaTopic ="perf4Topic"

  val brokerList = "localhost:9092,localhost:9093"

  val kafkaParams: Map[String, String] = Map(
    "metadata.broker.list" -> brokerList
  )

  val NumPublishers = 50

  val NumAdvertisers = 30

  val Publishers = (0 to NumPublishers).map("publisher" +)

  val Advertisers = (0 to NumAdvertisers).map("advertiser" +)

  val numProducerThreads = 1

  val UnknownGeo = "unknown"

  val Geos = Seq("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM",
    "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
    "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", UnknownGeo)

  val NumWebsites = 10000

  val NumCookies = 10000

  val totalNumLogs = 50000000;

  val batchDuration = Seconds(1)

  val topics = Set(kafkaTopic)
}
