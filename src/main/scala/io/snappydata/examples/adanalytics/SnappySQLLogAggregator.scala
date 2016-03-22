package io.snappydata.examples.adanalytics

import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.SnappyStreamingContext
import org.apache.spark.sql.{SaveMode, SnappyContext}
import org.apache.spark.streaming.{Seconds, StreamingContext, Milliseconds}
import org.apache.spark.streaming.kafka.KafkaUtils

object SnappySQLLogAggregator extends App {

  val sparkConf = new org.apache.spark.SparkConf()
    .setAppName(getClass.getSimpleName)
    .set("spark.sql.inMemoryColumnarStorage.compressed", "false")
    .set("spark.sql.inMemoryColumnarStorage.batchSize", "2000")
    // .setMaster("snappydata://localhost:10334")
    .setMaster("local[*]")


  val sc = new SparkContext(sparkConf)
  val snsc = SnappyStreamingContext(SnappyContext.getOrCreate(sc), Milliseconds(1000))
  // val ssc = new StreamingContext(sc, Seconds(1))

  val kafkaParams: Map[String, String] = Map(
    // "metadata.broker.list"->"localhost:9092,localhost:9093"
    "metadata.broker.list"->"localhost:9092"
  )

  val topics  = Set(Constants.kafkaTopic)
//  props.put("consumer.timeout.ms", "2000")
//  props.put("zookeeper.connect", zookeeper)
//  props.put("group.id", groupId)
//  props.put("zookeeper.session.timeout.ms", "400")
//  props.put("zookeeper.sync.time.ms", "10")
//  props.put("auto.commit.interval.ms", "1000")

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
    // " kafkaParams 'metadata.broker.list->localhost:9092,localhost:9093'," +
    " kafkaParams 'metadata.broker.list->localhost:9092'," +
    " topics 'adlogsTopic'," +
    " K 'java.lang.String'," +
    " V 'io.snappydata.examples.adanalytics.AdImpressionLog', " +
    " KD 'kafka.serializer.StringDecoder', " +
    " VD 'io.snappydata.examples.adanalytics.AdImpressionLogAvroDecoder')")

//  val messages = KafkaUtils.createDirectStream
//    [String, AdImpressionLog, StringDecoder, AdImpressionLogAvroDecoder](ssc, kafkaParams, topics).foreachRDD(rdd => { rdd })

  // snsc.getSchemaDStream("AdImpressionLog").foreachRDD(rdd => rdd)

  snsc.sql("create table adImpressions(timestamp long, publisher string, " +
      "advertiser string, website string, geo string, bid double, cookie string) " +
      "using column " +
      "options ( BUCKETS '29')")
//      // "options(PARTITION_BY 'timestamp')")
      // "options(PERSISTENT 'ASYNCHRONOUS')")
      // "options(PERSISTENT 'ASYNCHRONOUS', EVICTION_BY 'LRUMEMSIZE 600')")

  // val ingestTime = sc.accumulator(0l, "Actual time taken to ingest")
  snsc.getSchemaDStream("AdImpressionLog").foreachDataFrame(df => {
    // val start = System.currentTimeMillis()
    df.write.insertInto("adImpressions")
    // val end = System.currentTimeMillis()
    // println("Actual time taken to ingest the batch " + (end - start) + " millis")
  })


  /* snsc.sql("create table adImpressions(publisher string," +
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
    }) */

  snsc.start
  snsc.awaitTermination
}
