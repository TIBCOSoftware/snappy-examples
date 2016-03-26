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

import com.stratio.receiver.RabbitMQUtils
import io.snappydata.examples.adanalytics.Constants._
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.streaming.SnappyStreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}


object RabbitMQStream extends App {

  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  val snsc = SnappyStreamingContext(SnappyContext.getOrCreate(sc), batchDuration)
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  // Setup the receiver stream to connect to RabbitMQ.
  val receiverStream = RabbitMQUtils.createStream(snsc, Map(
    "host" -> "localhost",
    "queueName" -> "rabbitmq-q"
   //  "exchangeName" -> "rabbitmq-exchange"
   // "vHost" -> "rabbitmq-vHost"
   // "username" -> "rabbitmq-user",
   // "password" -> "rabbitmq-password",
   //  "x-max-length" -> "value",
   //  "x-max-length" -> "value",
   //  "x-message-ttl" -> "value",
   //  "x-expires" -> "value",
   //  "x-max-length-bytes" -> "value",
   //  "x-dead-letter-exchange" -> "value",
   //  "x-dead-letter-routing-key" -> "value",
   //  "x-max-priority" -> "value"
    ))

  // Start up the receiver.
  receiverStream.start()

  // Fires each time the configured window has passed.
  receiverStream.foreachRDD(r => {
    if (r.count() > 0) {
      // Do something with this message
      r.foreach( f => {println("Received " + f)} )
    }
    else {
      println("No new messages...")
    }
  })

  snsc.start
  snsc.awaitTermination
}
