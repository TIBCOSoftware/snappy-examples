package io.snappydata.examples.adanalytics

import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.memsql.spark.connector._
import com.memsql.spark.connector.util.JDBCImplicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.unsafe.types.UTF8String

object MemSqlLogAggregator extends App {

  val conf = new SparkConf().setAppName("MemSqlLogAggregator").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val msc = new MemSQLContext(sc)

  val ssc = new StreamingContext(sc, Seconds(1))
  val kafkaParams: Map[String, String] = Map(
    "metadata.broker.list"->"localhost:9092,localhost:9093"
  )

  val topics  = Set(Constants.memsqlaKafkaTopic)

 val rowConverter = new ImpressionLogToRow

  msc.getMemSQLCluster.withMasterConn(conn => {
    conn.withStatement(stmt => {
      stmt.execute(s"CREATE DATABASE IF NOT EXISTS logaggregator")
      stmt.execute(s"DROP TABLE IF EXISTS logaggregator.adimpressions")
      stmt.execute(s"""
                CREATE TABLE logaggregator.adimpressions
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

  val schema = msc.table("logaggregator.adimpressions").schema

  // msc.table("logaggregator.adimpressions").show()

  val messages = KafkaUtils.createDirectStream
    [String, AdImpressionLog, StringDecoder, AdImpressionLogAvroDecoder](ssc, kafkaParams, topics)
    .map(_._2).foreachRDD(rdd => {
    msc.createDataFrame(rowConverter.toRowRDD(rdd), schema).saveToMemSQL("logaggregator","adimpressions")
  })

  ssc.start
  ssc.awaitTermination
}

class ImpressionLogToRow {
  def toRowRDD(logRdd :RDD[AdImpressionLog]): RDD[Row] = {
    logRdd.map( log => {
      Row(log.getTimestamp,
        UTF8String.fromString(log.getPublisher.toString),
        UTF8String.fromString(log.getAdvertiser.toString),
        UTF8String.fromString(log.getWebsite.toString),
        UTF8String.fromString(log.getGeo.toString),
        log.getBid,
        UTF8String.fromString(log.getCookie.toString))
    })
  }
}
