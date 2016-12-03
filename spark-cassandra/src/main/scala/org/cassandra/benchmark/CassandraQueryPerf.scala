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

package org.cassandra.benchmark

import io.snappydata.adanalytics.Configs
import Configs._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object CassandraQueryPerf extends App {

  val rootLogger = Logger.getLogger("org");
  rootLogger.setLevel(Level.WARN);

  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
    .set("spark.cassandra.connection.host", s"$cassandraHost")
    .set("spark.cassandra.auth.username", "cassandra")
    .set("spark.cassandra.auth.password", "cassandra")
    .set("spark.cassandra.sql.keyspace", "adlogs")
    //    .set("spark.sql.shuffle.partitions", "8")
    .setMaster("local[*]")
    .set("spark.executor.cores", "2")
    .set("spark.ui.port", "4041")
  val assemblyJar = System.getenv("PROJECT_ASSEMBLY_JAR")
  if (assemblyJar != null) {
    conf.set("spark.driver.extraClassPath", assemblyJar)
    conf.set("spark.executor.extraClassPath", assemblyJar)
  }

//  val sc = new SparkContext(conf)
//  val msc = new CassandraSQLContext(sc)
  val sc = new SparkContext("spark://localhost:7077", "test", conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

 // sqlContext.setKeyspace("adlogs")

  var start = System.currentTimeMillis()
  sqlContext.sql("select count(*) AS adCount, geo from adimpressions group" +
    " by geo order by adCount desc limit 20").collect()
  println("Time for Q1 " + (System.currentTimeMillis() - start ))

  start = System.currentTimeMillis()
  sqlContext.sql("select sum (bid) as max_bid, geo from adimpressions group" +
    " by geo order by max_bid desc limit 20").collect()
  println("Time for Q2 " + (System.currentTimeMillis() - start ))

  start = System.currentTimeMillis()
  sqlContext.sql("select sum (bid) as max_bid, publisher from adimpressions" +
    " group by publisher order by max_bid desc limit 20").collect()
  println("Time for Q3 " + (System.currentTimeMillis() - start ))

  sqlContext.sql("select count(*) as cnt from adimpressions").show()

}
