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

import java.util.Properties

import io.snappydata.adanalytics.aggregator.Constants._
import io.snappydata.adanalytics.aggregator.KafkaAdImpressionGenerator._
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
  * A simple Kafka Producer program which randomly generates
  * ad impression log messages and sends it to Kafka broker.
  * This program generates and sends 3 million messages.
  */
object KafkaAdImpressionGenerator extends AdImpressionGenerator {

  val props = new Properties()
  props.put("producer.type", "async")
  props.put("request.required.acks", "0")
  props.put("serializer.class", "io.snappydata.adanalytics.aggregator.AdImpressionLogAvroEncoder")
  props.put("queue.buffering.max.messages", "1000000") // 10000
  props.put("metadata.broker.list", brokerList)
  props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
  props.put("key.serializer.class", "kafka.serializer.StringEncoder")
  props.put("batch.size", "9000000") // bytes
  props.put("linger.ms", "50")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, AdImpressionLog](config)

  def main(args: Array[String]) {
    println("Sending Kafka messages of topic " + kafkaTopic + " to brokers " + brokerList)
    for (i <- 1 to numProducerThreads) {
      new Thread(new Worker()).start()
    }
  }

  def sendToKafka(log: AdImpressionLog) = {
    producer.send(new KeyedMessage[String, AdImpressionLog](
      Constants.kafkaTopic, log.getTimestamp.toString, log))
  }
}

final class Worker extends Runnable {
  def run() {
    for (i <- 0 until totalNumLogs) {
      sendToKafka(generateAdImpression)
    }
  }
}
