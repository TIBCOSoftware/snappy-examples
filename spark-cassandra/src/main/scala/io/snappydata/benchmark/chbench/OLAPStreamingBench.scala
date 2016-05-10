package io.snappydata.benchmark.chbench

import java.io.PrintWriter

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.types.{IntegerType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object OLAPStreamingBench extends App {
  val rootLogger = Logger.getLogger("org");
  rootLogger.setLevel(Level.WARN);

  val host = "127.0.0.1"
  val master = "local[*]"
  val numWH = 10
  val memory = "8g"
  //val host = "172.31.13.52"
  //val master = "spark://ip-172-31-13-48:7077"
  //val numWH = 1000
  //val memory = "28g"

  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
    .set(s"spark.cassandra.connection.host", host)
    .set("spark.cassandra.auth.username", "cassandra")
    .set("spark.cassandra.auth.password", "cassandra")
    .set("spark.cassandra.sql.keyspace", "tpcc")
    .set("spark.driver.memory", memory)
    .set("spark.executor.memory",memory)
    .set("spark.executor.cores", "6")
    .set("spark.driver.maxResultSize", "10g")
    .set("spark.scheduler.mode", "FAIR")
    .setMaster(master)

  val assemblyJar = System.getenv("PROJECT_ASSEMBLY_JAR")
  if (assemblyJar != null) {
    conf.set("spark.driver.extraClassPath", assemblyJar)
    conf.set("spark.executor.extraClassPath", assemblyJar)
  }

  val sc = new SparkContext(conf)
  val cc = new CassandraSQLContext(sc)
  val ssc = new StreamingContext(sc, Duration(2000))

  cc.setKeyspace("tpcc")
  CassandraConnector(conf).withSessionDo { session =>
    println("******* CONNECTED TO CASSANDRA **********")
  }
  cc.sql("set spark.sql.shuffle.partitions=64")

  val stream = ssc.receiverStream[ClickStreamCustomer](
    new BenchmarkingReceiver(10000, numWH, 10, 30000, 100000))

  val schema = new StructType()
    .add("cs_c_w_id", IntegerType)
    .add("cs_c_d_id", IntegerType)
    .add("cs_c_id", IntegerType)
    .add("cs_i_id", IntegerType)
    .add("cs_timespent", IntegerType)
    .add("cs_click_d", TimestampType)

  val rows = stream.map(v => Row(v.w_id,
    v.d_id, v.c_id, v.i_id, v.c_ts, new java.sql.Timestamp(System.currentTimeMillis)))

  val window_rows = rows.window(new Duration(60 * 1000), new Duration(60 * 1000))

  window_rows.foreachRDD(rdd => {
    val df = cc.createDataFrame(rdd, schema)
    val outFileName = s"BenchmarkingStreamingJob-${System.currentTimeMillis()}.out"
    val pw = new PrintWriter(outFileName)
    val clickstreamlog = "benchmarking" + System.currentTimeMillis()
    df.registerTempTable(clickstreamlog)
    // Find out the items in the clickstream with
    // price range greater than a particular amount.
    var resultdfQ1: DataFrame = null
    var resultdfQ2: DataFrame = null
    cc.synchronized {
      resultdfQ1 = cc.sql(s"select i_id, count(i_id) from " +
        s" $clickstreamlog, item " +
        " where i_id = cs_i_id " +
        " AND i_price > 50 " +
        " GROUP BY i_id ");

      // Find out which district's customer are currently more online active to
      // stop tv commercials in those districts
      resultdfQ2 = cc.sql("select avg(cs_timespent) as avgtimespent , cs_c_d_id " +
        s"from $clickstreamlog group by cs_c_d_id order by avgtimespent")
    }

    val sq1 = System.currentTimeMillis()
    resultdfQ1.limit(10).collect().foreach(pw.println)
    val endq1 = System.currentTimeMillis()
    resultdfQ2.collect().foreach(pw.println)
    val endq2 = System.currentTimeMillis()
    val output = s"Q1 ${endq1 - sq1} Q2 ${endq2 - endq1}"
    val tid = Thread.currentThread()
    pw.println(s"$tid Time taken $output")
    pw.close()
  })

 ssc.start

  def getCurrentDirectory = new java.io.File(".").getCanonicalPath

  // scalastyle:off println

  var i: Int = 0
  while (i < 4) {
    val outFileName = s"HQueries-${i}.out"

    val pw = new PrintWriter(outFileName)

    i = i + 1
    for (q <- HQueries.queries) {
      val start: Long = System.currentTimeMillis
      val tid = Thread.currentThread()
      try {
        q._1 match {
          case "Q11" =>
            var df : DataFrame = null
            cc.synchronized {
              df = cc.sql(HQueries.Q11a)
            }
            val ret = df.collect()
            assert(ret.length == 1)
            val paramVal = ret(0).getDecimal(0)
            val qry = q._2.replace("?", paramVal.toString)
            cc.synchronized {
              df = cc.sql(qry)
            }
            df.collect()
          case "Q15" =>
            var ret : DataFrame = null
            cc.synchronized {
              ret = cc.sql(HQueries.Q15a)
            }
            ret.registerTempTable("revenue")
            var df : DataFrame = null
            cc.synchronized {
              df = cc.sql(HQueries.Q15b)
            }
            val maxV = df.collect()
            val paramVal = maxV(0).getDouble(0)
            val qry = q._2.replace("?", paramVal.toString)
            cc.synchronized {
              df = cc.sql(qry)
            }
            df.collect()
          case "Q22" =>
            var df: DataFrame = null
            cc.synchronized {
              df = cc.sql(HQueries.Q22a)
            }
            val ret = df.collect()
            assert(ret.length == 1)
            val paramVal = ret(0).getDouble(0)
            val qry = q._2.replace("?", paramVal.toString)
            cc.synchronized {
              df = cc.sql(qry)
            }
            df.collect()
          case "Q16" | "Q20" | "Q21" =>
            pw.println(s"$tid Not running " + q._1)
            pw.flush()
          //cc.sql(q._2).collect()
          case _ =>
            var df: DataFrame = null
            cc.synchronized {
              df = cc.sql(q._2)
            }
            df.collect()
        }
      } catch {
        case e: Throwable => pw.println(s"$tid Exception for query ${q._1}:  " + e)
          pw.println(e.getStackTraceString)
      }
      val end: Long = System.currentTimeMillis - start
      pw.println(s"${new java.util.Date(System.currentTimeMillis())} $tid Time taken by ${q._1} is $end")
      pw.flush()
    }
    pw.close()
  }
  //  // Return the output file name
  //  s"See ${getCurrentDirectory}"
  ssc.awaitTermination
}
