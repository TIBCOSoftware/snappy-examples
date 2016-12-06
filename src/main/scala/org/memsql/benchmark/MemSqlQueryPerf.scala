package org.memsql.benchmark

import java.io.PrintWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import com.memsql.spark.connector.MemSQLContext

object MemSqlQueryPerf extends App {

  val rootLogger = Logger.getLogger("org");
  rootLogger.setLevel(Level.WARN);

  val conf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    //.setMaster(s"$sparkMasterURL")
    .setMaster("local[*]")
    .set("spark.executor.cores", "2")

  val assemblyJar = System.getenv("PROJECT_ASSEMBLY_JAR")
  if (assemblyJar != null) {
    conf.set("spark.driver.extraClassPath", assemblyJar)
    conf.set("spark.executor.extraClassPath", assemblyJar)
  }
  conf.set("memsql.defaultDatabase", "adLogs")

  val sc = new SparkContext(conf)
  val msc = new MemSQLContext(sc)

  val outFileName = s"MemSQLQueryPerf-${System.currentTimeMillis()}.out"
  val pw = new PrintWriter(outFileName)

  var start = System.currentTimeMillis()
  msc.sql("select count(*) AS adCount, geo from adImpressions group" +
    " by geo order by adCount desc limit 20").collect()
  pw.println("Time for Q1 " + (System.currentTimeMillis() - start))
  pw.flush()

  start = System.currentTimeMillis()
  msc.sql("select sum (bid) as max_bid, geo from adImpressions group" +
    " by geo order by max_bid desc limit 20").collect()
  pw.println("Time for Q2 " + (System.currentTimeMillis() - start))
  pw.flush()

  start = System.currentTimeMillis()
  msc.sql("select sum (bid) as max_bid, publisher from adImpressions" +
    " group by publisher order by max_bid desc limit 20").collect()
  pw.println("Time for Q3 " + (System.currentTimeMillis() - start))
  pw.flush()

  start = System.currentTimeMillis()
  val array = msc.sql("select count(*) from adImpressions").collect()
  pw.println(array(0) +"Time for count(*) " + (System.currentTimeMillis() - start))
  pw.flush()
  pw.close()

}
