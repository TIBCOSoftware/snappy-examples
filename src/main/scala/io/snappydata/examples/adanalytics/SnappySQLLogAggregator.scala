package io.snappydata.examples.adanalytics

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.SnappyStreamingContext
import org.apache.spark.sql.{SaveMode, SnappyContext}
import org.apache.spark.streaming.Milliseconds

object SnappySQLLogAggregator extends App {

  val sparkConf = new org.apache.spark.SparkConf()
    .setAppName("SnappySQLLogAggregator")
    .setMaster("snappydata://localhost:10334")

  val sc = new SparkContext(sparkConf)
  val snsc = SnappyStreamingContext(SnappyContext.getOrCreate(sc), Milliseconds(1000))

  snsc.sql("drop table if exists AdImpressionLog")
  snsc.sql("drop table if exists adImpressions")
  snsc.sql("drop table if exists sampledAdImpressions")

  snsc.sql("create stream table AdImpressionLog (" +
    " timestamp long," +
    " publisher string," +
    " advertiser string," +
    " website string," +
    " geo string," +
    " bid double," +
    " cookie string) " +
    " using directkafka_stream options" +
    " (storagelevel 'MEMORY_AND_DISK_SER_2'," +
    " rowConverter 'io.snappydata.examples.adanalytics.KafkaStreamToRowsConverter' ," +
    " kafkaParams 'metadata.broker.list->localhost:9092,localhost:9093'," +
    " topics 'adnetwork-topic'," +
    " K 'java.lang.String'," +
    " V 'io.snappydata.examples.adanalytics.AdImpressionLog', " +
    " KD 'kafka.serializer.StringDecoder', " +
    " VD 'io.snappydata.examples.adanalytics.AdImpressionLogAvroDecoder')")

  snsc.getSchemaDStream("AdImpressionLog").foreachRDD(rdd => println(rdd.count))

  snsc.sql("create table adImpressions(publisher string," +
    " geo string, avg_bid double, imps long, uniques long) " +
    "using column " +
    "options(PARTITION_BY 'publisher')")

  snsc.sql("CREATE SAMPLE TABLE sampledAdImpressions (publisher string, geo string, avg_bid double, imps long, uniques long)" +
    " OPTIONS(qcs 'publisher', fraction '0.03', strataReservoirSize '50')")

  snsc.registerCQ("select publisher, geo, avg(bid) as avg_bid, count(*) imps, count(distinct(cookie)) uniques" +
    " from AdImpressionLog window (duration '2' seconds, slide '2' seconds)" +
    " where geo != 'unknown' group by publisher, geo").foreachDataFrame(df => {
      df.write.format("column").mode(SaveMode.Append)
        .options(Map.empty[String, String]).saveAsTable("adImpressions")
      df.write.format("sample").mode(SaveMode.Append)
        .options(Map.empty[String, String]).saveAsTable("sampledAdImpressions")
    })

  snsc.start
  snsc.awaitTermination
}
