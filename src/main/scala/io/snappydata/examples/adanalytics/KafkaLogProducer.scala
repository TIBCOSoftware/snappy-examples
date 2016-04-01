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
  props.put("producer.type", "async")
  props.put("request.required.acks", "0")
  props.put("serializer.class", "io.snappydata.examples.adanalytics.AdImpressionLogAvroEncoder")
//  props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
//  props.put("key.serializer.class", "kafka.serializer.StringEncoder")
  props.put("queue.buffering.max.messages", "1000000") // 10000
  props.put("metadata.broker.list", "192.168.1.92:9092,192.168.1.92:9093")

  props.put("batch.size", "9000000") // bytes
  props.put("linger.ms", "50") // ms

  //props.put("metadata.broker.list", "localhost:9092,localhost:9093") // one broker
  // props.put("batch.size", "100000") // bytes
  //props.put("linger.ms", "500") // ms
  //  props.put("queue.enqueue.timeout.ms" , "-1") // -1
//  props.put("queue.buffering.max.ms" , "500000") //500 sec
//  props.put("batch.num.messages" , "50000000") // 200
  // props.put("send.buffer.bytes" , "1024000")
  val config = new ProducerConfig(props)
  val producer = new Producer[String, AdImpressionLog](config)

  println("Sending Kafka messages...")

  def createLog() = {
    val random = new Random()
    val timestamp = System.currentTimeMillis()
    val publisher = Publishers(random.nextInt(NumPublishers))
    val advertiser = Advertisers(random.nextInt(NumAdvertisers))
//    val website = s"website_${random.nextInt(Constants.NumWebsites)}.com"
//    val cookie = s"cookie_${random.nextInt(Constants.NumCookies)}"
    val website = s"w${random.nextInt(Constants.NumWebsites)}"
    val cookie = s"c${random.nextInt(Constants.NumCookies)}"
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
    log
  }

  private def sendToKafka(adImpressionLog: AdImpressionLog) = {
    producer.send(new KeyedMessage[String, AdImpressionLog](
      Constants.kafkaTopic,
      //adImpressionLog.getTimestamp.toString,
      adImpressionLog))
  }

  class Worker extends Runnable {
    def run() {
      // infinite loop
      while (true) {
        sendToKafka(createLog)
      }
    }
    /*def run() {
      println(Thread.currentThread().getName + " running now")
      var startTime = System.currentTimeMillis()
      var i = 0
      // infinite loop
      while (true) {
        sendToKafka(createLog)
        val endTime = System.currentTimeMillis()
        val duration = endTime - startTime
        i = i + 1
        if (duration > 10000) {
          println(Thread.currentThread().getName + s" sent $i messages!")
          startTime = System.currentTimeMillis()
        }
      }
    }*/
  }
  for (i <- 1 to numProducerThreads) {
    new Thread(new Worker()).start()
  }
}