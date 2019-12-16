package io.snappydata.adanalytics

import io.snappydata.adanalytics.Configs._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{approx_count_distinct, avg, count, window}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Vanilla Spark implementation with no Snappy extensions being used.
  */
object SparkLogAggregator extends App {

  val sc = new SparkConf()
    .setAppName(getClass.getName)
    .setMaster("local[*]")
  val ssc = new StreamingContext(sc, Seconds(1))
  val schema = StructType(Seq(StructField("timestamp1", TimestampType), StructField("publisher", StringType),
    StructField("advertiser", StringType), StructField("website", StringType), StructField("geo", StringType),
    StructField("bid", DoubleType), StructField("cookie", StringType)))

  private val spark = SparkSession.builder().getOrCreate()

  import spark.implicits._

  //  private val snappy = new SnappySession(spark.sparkContext)
  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokerList)
    .option("value.deserializer", classOf[ByteArrayDeserializer].getName)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 100000)
    .option("subscribe", kafkaTopic)
    .load().select("value").as[Array[Byte]](Encoders.BINARY)
    .mapPartitions(itr => {
      val deserializer = new AdImpressionLogAVRODeserializer
      itr.map(data => {
        val adImpressionLog = deserializer.deserialize(data)
        Row(new java.sql.Timestamp(adImpressionLog.getTimestamp), adImpressionLog.getPublisher.toString,
          adImpressionLog.getAdvertiser.toString, adImpressionLog.getWebsite.toString,
          adImpressionLog.getGeo.toString, adImpressionLog.getBid, adImpressionLog.getCookie.toString)
      })
    })(RowEncoder.apply(schema))
    .filter(s"geo != '${Configs.UnknownGeo}'")

  // Group by on sliding window of 1 second
  val windowedDF = df.withColumn("eventTime", $"timestamp".cast("timestamp"))
    .withWatermark("eventTime", "10 seconds")
    .groupBy(window($"eventTime", "1 seconds", "1 seconds"),
      df.col("publisher"), df.col("geo"))
    .agg(avg("bid").alias("avg_bid"), count("geo").alias("imps"),
      approx_count_distinct("cookie").alias("uniques"))

  val logStream = windowedDF.select("window.start", "publisher", "geo", "imps", "uniques")
    .map(r => Row(r.getValuesMap(Seq("start", "publisher", "geo", "avg_bid", "imps", "uniques"))
      .mkString(",")))(RowEncoder(StructType(Seq(StructField("value", StringType, nullable = true)))))
    .writeStream
    .queryName("spark_log_aggregator")
    .option("checkpointLocation", sparkLogAggregatorCheckpointDir)
    .outputMode("update")
    .format("kafka")
    .option("kafka.bootstrap.servers", brokerList)
    .option("topic", "adImpressionsOut")
    .start()

  logStream.awaitTermination()
}
