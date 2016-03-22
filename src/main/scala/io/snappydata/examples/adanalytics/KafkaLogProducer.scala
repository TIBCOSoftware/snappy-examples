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

import java.util.Properties

import io.snappydata.examples.adanalytics.Constants._
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random

/**
  * A simple Kafka Producer program which randomly generates
  * ad impression log messages and sends it to Kafka broker.
  * This program generates and sends 30 million messages.
  */
object KafkaLogProducer extends App {

  val props = new Properties()
  props.put("metadata.broker.list", "localhost:9092") // one broker
  props.put("serializer.class", "io.snappydata.examples.adanalytics.AdImpressionLogAvroEncoder")
  // Asynchronous message publishing to Kafka
  props.put("producer.type", "async")
  val config = new ProducerConfig(props)
  val producer = new Producer[String, AdImpressionLog](config)

  println("Sending Kafka messages...")
  val random = new Random()
  val startTime = System.currentTimeMillis()
  var logCount = 0
  while (logCount <= totalNumLogs) {
    val timestamp = System.currentTimeMillis()
    val publisher = Publishers(random.nextInt(NumPublishers))
    val advertiser = Advertisers(random.nextInt(NumAdvertisers))
    val website = s"website_${random.nextInt(Constants.NumWebsites)}.com"
    val cookie = s"cookie_${random.nextInt(Constants.NumCookies)}"
    val geo = Geos(random.nextInt(Geos.size))
    val bid = math.abs(random.nextDouble()) % 1
    val log = new AdImpressionLog()
    log.setTimestamp(timestamp)
    log.setPublisher(publisher)
    log.setAdvertiser(advertiser)
    log.setWebsite(website)
    log.setGeo(geo)
    log.setBid(bid)
    log.setCookie(cookie)
    producer.send(new KeyedMessage[String, AdImpressionLog](Constants.kafkaTopic, log))
    logCount += 1
    if (logCount % 100000 == 0) {
      println(s"KafkaProducer published total $logCount messages")
    }
  }
  val timeTaken = System.currentTimeMillis() - startTime
  printf(s"Total time taken to publish " + totalNumLogs + " messages is " + timeTaken + " milliseconds")
}


