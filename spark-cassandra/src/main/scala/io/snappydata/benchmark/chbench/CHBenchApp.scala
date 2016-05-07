package io.snappydata.benchmark.chbench

import java.io.PrintWriter
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object CHBenchApp extends App {

  val rootLogger = Logger.getLogger("org");
  rootLogger.setLevel(Level.ERROR);

  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.cassandra.auth.username", "cassandra")
    .set("spark.cassandra.auth.password", "cassandra")
    .set("spark.cassandra.sql.keyspace", "newtpcc")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)
  val cc = new CassandraSQLContext(sc)
  val ssc = new StreamingContext(sc, Duration(2000))

  CassandraConnector(conf).withSessionDo { session =>
    println("************CONNECTED TO CASSANDRA V8  *******************************")
  }

  cc.setKeyspace("tpcc")

  //    val schema = new StructType()
  //          .add("cs_c_w_id", IntegerType)
  //          .add("cs_c_d_id", IntegerType)
  //          .add("cs_c_id", IntegerType)
  //          .add("cs_i_id", IntegerType)
  //          .add("cs_click_d", TimestampType)
  //
  //    val stream = ssc.receiverStream[ClickStreamCustomer](
  //              new BenchmarkingReceiver(10000,1,10,30000,100000))
  //
  //    val rows = stream.map(v => Row(new java.sql.Timestamp(System.currentTimeMillis), v.w_id, v.d_id, v.c_id, v.i_id))
  //    val window_rows = rows.window(new Duration(2000), new Duration(2000))
  //
  //    import com.datastax.spark.connector.streaming._
  //
  //    rows.saveToCassandra("tpcc", "clickstream", SomeColumns("cs_c_w_id","cs_c_d_id", "cs_c_id","cs_i_id","cs_click_d"))
  //
  //    ssc.start
  //    ssc.awaitTermination

  def getCurrentDirectory = new java.io.File(".").getCanonicalPath

  // scalastyle:off println
  cc.sql("set spark.sql.shuffle.partitions=20")
  var i: Int = 0
  while (i < 4) {
    val outFileName = s"HQueries-${i}.out"

    val pw = new PrintWriter(outFileName)

    i = i + 1
    for (q <- HQueries.queries) {
      val start: Long = System.currentTimeMillis
      try {
        q._1 match {
          case "Q11" =>
            val ret = cc.sql(HQueries.Q11a).collect()
            assert(ret.length == 1)
            val paramVal = ret(0).getDecimal(0)
            val qry = q._2.replace("?", paramVal.toString)
            cc.sql(qry).collect()
          case "Q15" =>
            var ret = cc.sql(HQueries.Q15a)
            ret.registerTempTable("revenue")
            val maxV = cc.sql(HQueries.Q15b).collect()
            val paramVal = maxV(0).getDouble(0)
            val qry = q._2.replace("?", paramVal.toString)
            cc.sql(qry).collect()
          case "Q22" =>
            val ret = cc.sql(HQueries.Q22a).collect()
            assert(ret.length == 1)
            val paramVal = ret(0).getDouble(0)
            val qry = q._2.replace("?", paramVal.toString)
            cc.sql(qry).collect()
          case _ =>
            pw.println("About to run " + q._1)
            pw.flush()
            cc.sql(q._2).collect()
        }
      } catch {
        case e: Throwable => pw.println(s"Exception for query ${q._1}:  " + e)
                             pw.println(e.getStackTraceString)
      }
      val end: Long = System.currentTimeMillis - start
      pw.println(s"${new java.util.Date(System.currentTimeMillis())} Time taken by ${q._1} is $end")
      pw.flush()
    }
    pw.close()
  }
  // Return the output file name
  s"See ${getCurrentDirectory}"

}
