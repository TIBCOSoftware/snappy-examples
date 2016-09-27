package io.snappydata.benchmark

import com.typesafe.config.Config
import io.snappydata.adanalytics.Configs._
import org.apache.spark.sql.streaming.SnappyStreamingJob
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation}
import org.apache.spark.streaming.SnappyStreamingContext

class SnappyStreamIngestPerfJob extends SnappyStreamingJob {

  override def runSnappyJob(snsc: SnappyStreamingContext, jobConfig: Config): Any = {
    //snsc.sql("drop table if exists adImpressions")
    snsc.snappySession.sql("drop table if exists adImpressionStream")

    // Create a stream of AdImpressionLog which will pull the log messages
    // from Kafka broker
    snsc.snappySession.sql("create stream table adImpressionStream (" +
      " time_stamp timestamp," +
      " publisher string," +
      " advertiser string," +
      " website string," +
      " geo string," +
      " bid double," +
      " cookie string) " +
      " using directkafka_stream options (" +
      " rowConverter 'io.snappydata.adanalytics.AdImpressionToRowsConverter' ," +
      s" kafkaParams 'metadata.broker.list->$brokerList'," +
      s" topics '$kafkaTopic'," +
      " K 'java.lang.String'," +
      " V 'io.snappydata.adanalytics.AdImpressionLog', " +
      " KD 'kafka.serializer.StringDecoder', " +
      " VD 'io.snappydata.adanalytics.AdImpressionLogAvroDecoder')")

    snsc.snappySession.sql("create table adImpressions(times_tamp timestamp, publisher string, " +
      "advertiser string, website string, geo string, bid double, cookie string) " +
      "using column " +
      "options ( buckets '29')")

    snsc.snappySession.sql("CREATE SAMPLE TABLE sampledAdImpressions" +
      " OPTIONS(qcs 'geo,publisher', fraction '0.02', strataReservoirSize '50', baseTable 'adImpressions')")

    // Save the streaming data to snappy store per second (btachDuration)
    snsc.getSchemaDStream("adImpressionStream").foreachDataFrame( df => {
        df.write.insertInto("adImpressions")
        df.write.insertInto("sampledAdImpressions")
      })

    snsc.start
    snsc.awaitTermination
  }

  override def isValidJob(snsc: SnappyStreamingContext, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }
}