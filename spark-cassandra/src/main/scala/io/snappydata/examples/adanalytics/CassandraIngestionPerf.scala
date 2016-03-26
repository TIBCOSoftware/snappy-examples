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

import com.datastax.spark.connector.cql.CassandraConnector
import io.snappydata.examples.adanalytics.Constants._
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Simple direct kafka spark streaming program which pulls log messages
  * from kafka broker and ingest those log messages to Cassandra using
  * DataStax's Spark Cassandra Connector. To run this program you need to
  * start a single instance of Cassandra and run Spark in local mode.
  */
object CassandraIngestionPerf extends App {

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host" , "127.0.0.1")
    .set("spark.cassandra.auth.username" , "cassandra")
    .set("spark.cassandra.auth.password" , "cassandra")
    .set("spark.streaming.kafka.maxRatePerPartition" , "40000")
  val sc = new SparkContext("local[*]", getClass.getSimpleName, conf)

  CassandraConnector(conf).withSessionDo { session =>
    // Create keysapce and table in Cassandra
    session.execute(s"DROP KEYSPACE IF EXISTS adlogs")
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS adlogs " +
      s"WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(s"CREATE TABLE IF NOT EXISTS adlogs.adimpressions " +
      s"(timestamp bigint primary key, publisher text, advertiser text, " +
      "website text, geo text, bid double, cookie text)")
  }
  // batchDuration of 1 second
  val ssc = new StreamingContext(sc, batchDuration)

  // Creates a stream of AdImpressionLog using kafka direct that pulls
  // messages from a Kafka Broker
  val messages = KafkaUtils.createDirectStream
    [String, AdImpressionLog, StringDecoder, AdImpressionLogAvroDecoder](ssc, kafkaParams, topics)

  import com.datastax.spark.connector._ // for saveToCassandra implicit

  // transform the Spark RDDs as per the table schema and save it to Cassandra
  messages.foreachRDD(rdd => rdd.map({
    case (topic, adImp) => (adImp.getTimestamp, adImp.getPublisher,
      adImp.getAdvertiser, adImp.getWebsite, adImp.getGeo, adImp.getBid, adImp.getCookie)
  }).saveToCassandra("adlogs", "adimpressions",
    SomeColumns("timestamp", "publisher", "advertiser", "website", "geo", "bid", "cookie"))
  )

  ssc.start
  ssc.awaitTermination
}
