package io.snappydata.benchmark.chbench

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.types.{IntegerType, StructType, TimestampType}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingBench extends App {
  val rootLogger = Logger.getLogger("org");
  rootLogger.setLevel(Level.ERROR);

  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.cassandra.auth.username", "cassandra")
    .set("spark.cassandra.auth.password", "cassandra")
    .set("spark.cassandra.sql.keyspace", "tpcc")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)
  val cc = new CassandraSQLContext(sc)
  val ssc = new StreamingContext(sc, Duration(2000))

  CassandraConnector(conf).withSessionDo { session =>
    println("********CONNECTED TO CASSANDRA V8  *************")
  }
  cc.sql("set spark.sql.shuffle.partitions=64")
  cc.setKeyspace("tpcc")

  val schema = new StructType()
    .add("cs_c_w_id", IntegerType)
    .add("cs_c_d_id", IntegerType)
    .add("cs_c_id", IntegerType)
    .add("cs_i_id", IntegerType)
    .add("cs_click_d", TimestampType)

  val stream = ssc.receiverStream[ClickStreamCustomer](
    new BenchmarkingReceiver(10000, 1, 10, 30000, 100000))

  val rows = stream.map(v => Row(new java.sql.Timestamp(System.currentTimeMillis), v.w_id, v.d_id, v.c_id, v.i_id))
  val window_rows = rows.window(new Duration(2000), new Duration(2000))
  //window_rows.foreachRDD(rdd => println(rdd.count))

  import com.datastax.spark.connector.streaming._

  rows.saveToCassandra("tpcc", "clickstream", SomeColumns("cs_c_w_id","cs_c_d_id", "cs_c_id","cs_i_id","cs_click_d"))

  ssc.start
  ssc.awaitTermination
}
