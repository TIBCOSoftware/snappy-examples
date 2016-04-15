package io.snappydata.adanalytics.aggregator

import com.typesafe.config.Config
import io.snappydata.adanalytics.aggregator.Constants._
import kafka.serializer.StringDecoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.SnappyStreamingJob
import org.apache.spark.sql.types._
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.kafka.KafkaUtils
import spark.jobserver.{SparkJobValid, SparkJobValidation}

class SnappyAPILogAggregatorJob extends SnappyStreamingJob {

  override def runJob(snsc: C, jobConfig: Config): Any = {
    // stream of (topic, ImpressionLog)
    val messages = KafkaUtils.createDirectStream
      [String, AdImpressionLog, StringDecoder, AdImpressionLogAvroDecoder](snsc, kafkaParams, topics)

    // Filter out bad messages ...use a 2 second window
    val logs = messages.map(_._2).filter(_.getGeo != Constants.UnknownGeo)
      .window(Duration.apply(2000), Duration.apply(2000))

    // Best to operate stream as a DataFrame/Table ... easy to run analytics on stream
    val rows = logs.map(v => Row(new java.sql.Timestamp(v.getTimestamp), v.getPublisher.toString,
      v.getAdvertiser.toString, v.getWebsite.toString, v.getGeo.toString, v.getBid, v.getCookie.toString))

    val logStreamAsTable = snsc.createSchemaDStream(rows, getAdImpressionSchema)

    import org.apache.spark.sql.functions._

    /**
      * We want to execute the following analytic query ... using the DataFrame
      * API ...
      * select publisher, geo, avg(bid) as avg_bid, count(*) imps, count(distinct(cookie)) uniques
      * from AdImpressionLog group by publisher, geo, timestamp"
      */
    logStreamAsTable.foreachDataFrame(df => {
      val df1 = df.groupBy("publisher", "geo", "timestamp")
        .agg(avg("bid").alias("avg_bid"), count("geo").alias("imps"),
          countDistinct("cookie").alias("uniques"))
      df1.show()
    })

    snsc.start
  }

  private def getAdImpressionSchema: StructType = {
    StructType(Array(
      StructField("timestamp", TimestampType, true),
      StructField("publisher", StringType, true),
      StructField("advertiser", StringType, true),
      StructField("website", StringType, true),
      StructField("geo", StringType, true),
      StructField("bid", DoubleType, true),
      StructField("cookie", StringType, true)))
  }

  override def validate(snsc: C, config: Config): SparkJobValidation = {
    SparkJobValid
  }
}