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

import java.io.PrintWriter

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CassandraQueryPerf extends App {

  val rootLogger = Logger.getLogger("org")
  rootLogger.setLevel(Level.WARN)

  val ss = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[*]")
    .config("spark.executor.cores", "2")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .getOrCreate()

  val outFileName = s"CassandraQueryPerf-${System.currentTimeMillis()}.out"
  val pw = new PrintWriter(outFileName)

  // ss.sql("CREATE TABLE geoBids (geo string, max_bid double)")

  val df = ss
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "adimpressions", "keyspace" -> "adlogs"))
    .load()

  df.createOrReplaceTempView("adImpressions")

  var start = System.currentTimeMillis()
  ss.sql("select count(*) AS adCount, geo from adImpressions group" +
    " by geo order by adCount desc limit 20").collect()
  pw.println("Time for Q1 " + (System.currentTimeMillis() - start))
  pw.flush()

  start = System.currentTimeMillis()
  val result = ss.sql("select sum (bid) as max_bid, geo from adImpressions group" +
    " by geo order by max_bid desc limit 20").collect()
  pw.println("Time for Q2 " + (System.currentTimeMillis() - start))
  pw.flush()

  val maxBid = result.toList(1).getDouble(0)
  val geo = result.toList(1).getString(1)

  start = System.currentTimeMillis()

//  CassandraConnector(ss.sparkContext.getConf).withSessionDo { session =>
//    session.execute(s"INSERT INTO adLogs.geoBids(geo, max_bid) VALUES ($geo, 0.0)")
//    // Configs.geos.foreach(geo => session.execute(s"INSERT INTO adLogs.geoBids(geo, max_bid) VALUES ($geo, 0.0)"))
//  }

  CassandraConnector(ss.sparkContext.getConf).withSessionDo { session =>
    session.execute(s"update adLogs.geoBids set max_bid=$maxBid where geo='$geo'")
  }

  // ss.sql(s"update table geoBids set max_bid=$maxBid where geo='$geo'")
  pw.println("Time for update " + (System.currentTimeMillis() - start))
  pw.flush()

  start = System.currentTimeMillis()
  ss.sql("select sum (bid) as max_bid, publisher from adImpressions" +
    " group by publisher order by max_bid desc limit 20").collect()
  pw.println("Time for Q3 " + (System.currentTimeMillis() - start))
  pw.flush()

  start = System.currentTimeMillis()
  val array = ss.sql("select count(*) from adImpressions").collect()
  pw.println(array(0) +"Time for count(*) " + (System.currentTimeMillis() - start))
  pw.flush()
  pw.close()
}
