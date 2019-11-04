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

import java.util.Properties
import java.util.concurrent.Future

import io.snappydata.adanalytics.Configs._
import io.snappydata.adanalytics.KafkaAdImpressionProducer._
import org.apache.hadoop.ipc.RetriableException
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

/**
  * A simple Kafka Producer program which randomly generates
  * ad impression log messages and sends it to Kafka broker.
  * This program generates and sends 10 million messages.
  *
  * Note that this producer sends messages on Kafka in async manner.
  */
object KafkaAdImpressionProducer {

  val props = new Properties()
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "io.snappydata.adanalytics.AdImpressionLogAVROSerializer")
  props.put("bootstrap.servers", "localhost:9092")

  val producer = new KafkaProducer[String, AdImpressionLog](props)

  def main(args: Array[String]) {
    println("Sending Kafka messages of topic " + kafkaTopic + " to brokers " + brokerList)
    val threads = new Array[Thread](numProducerThreads)
    for (i <- 0 until numProducerThreads) {
      val thread = new Thread(new Worker())
      thread.start()
      threads(i) = thread
    }
    threads.foreach(_.join())
    println(s"Done sending $numLogsPerThread Kafka messages of topic $kafkaTopic")
    System.exit(0)
  }

  def sendToKafka(log: AdImpressionLog): Future[RecordMetadata] = {
    producer.send(new ProducerRecord[String, AdImpressionLog](
      Configs.kafkaTopic, log.getTimestamp.toString, log), new org.apache.kafka.clients.producer.Callback() {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          if (exception.isInstanceOf[RetriableException]) {
            println(s"Encountered a retriable exception while sending messages: $exception")
          } else {
            throw exception
          }
        }
      }
    }
    )
  }
}

final class Worker extends Runnable {
  def run() {
    for (j <- 0 to numLogsPerThread by maxLogsPerSecPerThread) {
      val start = System.currentTimeMillis()
      for (i <- 1 to maxLogsPerSecPerThread) {
        sendToKafka(AdImpressionGenerator.nextNormalAdImpression())
      }
      // If one second hasn't elapsed wait for the remaining time
      // before queueing more.
      val timeRemaining = 1000 - (System.currentTimeMillis() - start)
      if (timeRemaining > 0) {
        Thread.sleep(timeRemaining)
      }
      if (j != 0 & (j % 200000) == 0) {
        println(s" ${Thread.currentThread().getName} sent $j Kafka messages" +
          s" of topic $kafkaTopic to brokers $brokerList ")
      }
    }
  }
}
