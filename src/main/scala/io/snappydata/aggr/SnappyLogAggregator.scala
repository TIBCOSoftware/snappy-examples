package io.snappydata.aggr

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.SnappyStreamingContext
import org.apache.spark.sql.{SaveMode, SnappyContext}
import org.apache.spark.streaming.Milliseconds

object SnappyLogAggregator extends App {

  val sparkConf = new org.apache.spark.SparkConf()
    .setAppName("logAggregator")
    .setMaster("snappydata://localhost:10334")

  val sc = new SparkContext(sparkConf)
  val snsc = SnappyStreamingContext(SnappyContext.getOrCreate(sc), Milliseconds(10000))

  snsc.sql("drop table if exists impressionlog")
  snsc.sql("drop table if exists snappyStoreTable")

  snsc.sql("create stream table impressionlog (timestamp long, publisher string," +
    " advertiser string, " +
    "website string, geo string, bid double, cookie string) " +
    "using directkafka_stream options " +
    "(storagelevel 'MEMORY_AND_DISK_SER_2', " +
    "rowConverter 'io.snappydata.aggr.KafkaStreamToRowsConverter' ," +
    " kafkaParams 'metadata.broker.list->localhost:9092,localhost:9093'," +
    " topics 'adnetwork-topic'," +
    " K 'java.lang.String'," +
    " V 'io.snappydata.aggr.ImpressionLog', " +
    " KD 'kafka.serializer.StringDecoder', " +
    " VD 'io.snappydata.aggr.ImpressionLogAvroDecoder')")

  snsc.sql("create table snappyStoreTable(publisher string," +
    " geo string, avg_bid double, imps long, uniques long) " +
    "using column " +
    "options(PARTITION_BY 'publisher')")

  snsc.registerCQ("select publisher, geo, avg(bid) as avg_bid, count(*) imps, count(distinct(cookie)) uniques" +
    " from impressionlog window (duration '10' seconds, slide '10' seconds)" +
    " where geo != 'unknown' group by publisher, geo")
    .foreachDataFrame(df => {
      df.show
      df.write.format("column").mode(SaveMode.Append)
        .options(Map.empty[String, String]).saveAsTable("snappyStoreTable")
    })
  snsc.start
  snsc.awaitTermination
}