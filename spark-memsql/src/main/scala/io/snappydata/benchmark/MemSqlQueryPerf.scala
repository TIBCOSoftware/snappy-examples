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

import com.memsql.spark.connector.MemSQLContext
import io.snappydata.adanalytics.Configs._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MemSqlQueryPerf extends App {

  val rootLogger = Logger.getLogger("org");
  rootLogger.setLevel(Level.WARN);

  val conf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    .setMaster(s"$sparkMasterURL")

  val assemblyJar = System.getenv("PROJECT_ASSEMBLY_JAR")
  if (assemblyJar != null) {
    conf.set("spark.driver.extraClassPath", assemblyJar)
    conf.set("spark.executor.extraClassPath", assemblyJar)
  }
  conf.set("memsql.defaultDatabase", "adLogs")

  val sc = new SparkContext(conf)
  val msc = new MemSQLContext(sc)

  var start = System.currentTimeMillis()
  msc.sql("select count(*) AS adCount, geo from adImpressions group" +
    " by geo order by adCount desc limit 20").collect()
  println("Time for Q1 " + (System.currentTimeMillis() - start))

  start = System.currentTimeMillis()
  msc.sql("select sum (bid) as max_bid, geo from adImpressions group" +
    " by geo order by max_bid desc limit 20").collect()
  println("Time for Q2 " + (System.currentTimeMillis() - start))

  start = System.currentTimeMillis()
  msc.sql("select sum (bid) as max_bid, publisher from adImpressions" +
    " group by publisher order by max_bid desc limit 20").collect()
  println("Time for Q3 " + (System.currentTimeMillis() - start))

}


