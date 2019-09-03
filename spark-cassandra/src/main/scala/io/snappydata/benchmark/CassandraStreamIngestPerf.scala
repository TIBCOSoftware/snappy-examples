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

package io.snappydata.benchmark

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import io.snappydata.adanalytics.AdImpressionLogAvroDecoder
import io.snappydata.adanalytics.Configs._
import io.snappydata.adanalytics.AdImpressionLog
import io.snappydata.adanalytics.SnappyAPILogAggregator.ssc
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Simple direct kafka spark streaming program which pulls log messages
  * from kafka broker and ingest those log messages to Cassandra using
  * DataStax's Spark Cassandra Connector. To run this program you need to
  * start a single instance of Cassandra and run Spark in local mode.
  */
object CassandraStreamIngestPerf extends App {

  val rootLogger = Logger.getLogger("org");
  rootLogger.setLevel(Level.WARN);

  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
    .set("spark.cassandra.connection.host", s"$cassandraHost")
    .set("spark.cassandra.auth.username", "cassandra")
    .set("spark.cassandra.auth.password", "cassandra")
    .set("spark.streaming.kafka.maxRatePerPartition", s"$maxRatePerPartition")
    .set("spark.cassandra.output.batch.size.bytes", "5120") //8000 * 1024
    .set("spark.cassandra.output.concurrent.writes", "32")
    .set("spark.cassandra.output.consistency.level", "ANY")
    .set("spark.cassandra.output.batch.grouping.key", "none") ///replica_set/partition
    .set("spark.cassandra.sql.keyspace", "adlogs")
    .set("spark.executor.cores", "2")
    //.set("spark.cassandra.output.batch.size.rows", "500")
    //.set("spark.cassandra.output.batch.grouping.buffer.size", "1") //1000
    //.setMaster(s"$sparkMasterURL")
    .setMaster("local[*]")

  val assemblyJar = System.getenv("PROJECT_ASSEMBLY_JAR")
  if (assemblyJar != null) {
    conf.set("spark.driver.extraClassPath", assemblyJar)
    conf.set("spark.executor.extraClassPath", assemblyJar)
  }
  val sc = new SparkContext(conf)
  val csc = new CassandraSQLContext(sc)
  CassandraConnector(conf).withSessionDo { session =>
    // Create keysapce and table in Cassandra
    session.execute(s"DROP KEYSPACE IF EXISTS adlogs")
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS adlogs " +
      s"WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(s"CREATE TABLE IF NOT EXISTS adlogs.adimpressions " +
      s"(timestamp bigint, publisher text, advertiser text, " +
      "website text, geo text, bid double, cookie text, primary key (timestamp, cookie))")
  }
  csc.setKeyspace("adlogs")

  // batchDuration of 1 second
  val ssc = new StreamingContext(sc, batchDuration)

  // Creates a stream of AdImpressionLog using kafka direct that pulls
  // messages from a Kafka Broker
  val messages = KafkaUtils.createDirectStream
    [String, AdImpressionLog, StringDecoder, AdImpressionLogAvroDecoder](ssc, kafkaParams, topics)

  import com.datastax.spark.connector.streaming._

  messages.map(_._2).map(ad =>
    (ad.getTimestamp, ad.getPublisher, ad.getAdvertiser, ad.getWebsite, ad.getGeo, ad.getBid, ad.getCookie))
    .saveToCassandra("adlogs", "adimpressions",
      SomeColumns("timestamp", "publisher", "advertiser", "website", "geo", "bid", "cookie"))

  ssc.start
  ssc.awaitTermination
}
