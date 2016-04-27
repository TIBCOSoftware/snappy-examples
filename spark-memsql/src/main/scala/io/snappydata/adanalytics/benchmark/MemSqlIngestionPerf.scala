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

package io.snappydata.adanalytics.benchmark

import com.memsql.spark.connector.MemSQLContext
import io.snappydata.adanalytics.aggregator.Constants._
import io.snappydata.adanalytics.aggregator.{AdImpressionLog, AdImpressionLogAvroDecoder, AdImpressionLogToRowRDD}
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Simple direct kafka spark streaming program which pulls log messages
  * from kafka broker and ingest those log messages to MemSql using
  * Spark MemSql Connector. To run this program you need to
  * start an aggregator and leaf node of MemSql and run Spark in local mode.
  */
object MemSqlIngestionPerf extends App {

  Logger.getRootLogger().setLevel(Level.ERROR)

  val conf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    .setMaster("local[*]")

  val assemblyJar = System.getenv("PROJECT_ASSEMBLY_JAR")
  if (assemblyJar != null) {
    conf.set("spark.driver.extraClassPath", assemblyJar)
    conf.set("spark.executor.extraClassPath", assemblyJar)
  }
  conf.set("memsql.defaultDatabase", "adLogs")

  val sc = new SparkContext(conf)
  val msc = new MemSQLContext(sc)

  import com.memsql.spark.connector.util.JDBCImplicits._

  msc.getMemSQLCluster.withMasterConn(conn => {
    conn.withStatement(stmt => {
      // Create database and table in MemSql
      stmt.execute(s"CREATE DATABASE IF NOT EXISTS adLogs")
      stmt.execute(s"DROP TABLE IF EXISTS adLogs.adImpressions")
      stmt.execute(
        s"""
                CREATE TABLE adLogs.adImpressions
                (timestamp bigint,
                publisher varchar(15),
                advertiser varchar(15),
                website varchar(20),
                geo varchar(4),
                bid double,
                cookie varchar(20),
                SHARD KEY (timestamp))
              """)
    })
  })

  // batchDuration of 1 second
  val ssc = new StreamingContext(sc, batchDuration)

  val schema = msc.table("adLogs.adImpressions").schema

  val rowConverter = new AdImpressionLogToRowRDD

  import com.memsql.spark.connector._

  // Creates a stream of AdImpressionLog using kafka direct that pulls
  // messages from a Kafka Broker
  val messages = KafkaUtils.createDirectStream
    [String, AdImpressionLog, StringDecoder, AdImpressionLogAvroDecoder](ssc, kafkaParams, topics)

  // transform the Spark RDDs as per the table schema and save it to MemSql
  messages.map(_._2).foreachRDD(rdd => {
    msc.createDataFrame(rowConverter.convert(rdd), schema)
      .saveToMemSQL("adLogs", "adImpressions")
  })

  ssc.start
  ssc.awaitTermination
}


