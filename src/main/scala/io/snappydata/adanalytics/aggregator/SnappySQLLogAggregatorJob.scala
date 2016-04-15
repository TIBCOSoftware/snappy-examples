package io.snappydata.adanalytics.aggregator

import com.typesafe.config.Config
import io.snappydata.adanalytics.aggregator.Constants._
import org.apache.spark.sql.streaming.SnappyStreamingJob
import spark.jobserver.{SparkJobValid, SparkJobValidation}


class SnappySQLLogAggregatorJob extends SnappyStreamingJob {

  override def runJob(snsc: C, jobConfig: Config): Any = {
    snsc.sql("drop table if exists adImpressionStream")
    snsc.sql("drop table if exists aggrAdImpressions")

    snsc.sql("create stream table adImpressionStream (" +
      " time_stamp timestamp," +
      " publisher string," +
      " advertiser string," +
      " website string," +
      " geo string," +
      " bid double," +
      " cookie string) " +
      " using directkafka_stream options" +
      " (storagelevel 'MEMORY_AND_DISK_SER_2'," +
      " rowConverter 'io.snappydata.adanalytics.aggregator.AdImpressionToRowsConverter' ," +
      " kafkaParams 'metadata.broker.list->localhost:9092,localhost:9093'," +
      s" topics '$kafkaTopic'," +
      " K 'java.lang.String'," +
      " V 'io.snappydata.adanalytics.aggregator.AdImpressionLog', " +
      " KD 'kafka.serializer.StringDecoder', " +
      " VD 'io.snappydata.adanalytics.aggregator.AdImpressionLogAvroDecoder')")

    snsc.sql("create table aggrAdImpressions(time_stamp timestamp, publisher string," +
      " geo string, avg_bid double, imps long, uniques long) " +
      "using column options(buckets '29')")

    snsc.registerCQ("select time_stamp, publisher, geo, avg(bid) as avg_bid," +
      " count(*) imps, count(distinct(cookie)) uniques" +
      " from adImpressionStream window (duration '2' seconds, slide '2' seconds)" +
      " where geo != 'unknown' group by publisher, geo, time_stamp")

      .foreachDataFrame(df => {
        df.write.insertInto("aggrAdImpressions")
      })

    snsc.start
  }

  override def validate(snsc: C, config: Config): SparkJobValidation = {
    SparkJobValid
  }
}
