package io.snappydata.benchmark

import io.snappydata.adanalytics.Configs
import Configs._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.cassandra.CassandraSQLContext
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

  val sc = new SparkContext(conf)
  val msc = new CassandraSQLContext(sc)
  msc.setKeyspace("adlogs")
  var start = System.currentTimeMillis()
  msc.sql("select count(*) AS adCount, geo from adimpressions group" +
    " by geo order by adCount desc limit 20").collect()
  println("Time for Q1 " + (System.currentTimeMillis() - start ))

  start = System.currentTimeMillis()
  msc.sql("select sum (bid) as max_bid, geo from adimpressions group" +
    " by geo order by max_bid desc limit 20").collect()
  println("Time for Q2 " + (System.currentTimeMillis() - start ))

  start = System.currentTimeMillis()
  msc.sql("select sum (bid) as max_bid, publisher from adimpressions" +
    " group by publisher order by max_bid desc limit 20").collect()
  println("Time for Q3 " + (System.currentTimeMillis() - start ))

  msc.sql("select count(*) as cnt from adimpressions").show()

}
