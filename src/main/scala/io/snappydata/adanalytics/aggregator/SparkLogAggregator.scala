package io.snappydata.adanalytics.aggregator

import com.twitter.algebird.{HLL, HyperLogLogMonoid}
import io.snappydata.adanalytics.aggregator.Configs._
import kafka.serializer.StringDecoder
import org.apache.commons.io.Charsets
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.DateTime

/**
  * Vanilla Spark implementation with no Snappy extensions being used.
  * Code is from https://chimpler.wordpress.com/2014/07/01/implementing-a-real-time-data-pipeline-with-spark-streaming/
  * This implementation uses a HyperLogLog to find uniques. We skip this
  * probabilistic structure in our implementation as we can easily extract the
  * exact distinct count for such small time windows.
  **/
object SparkLogAggregator extends App {

  val sc = new SparkConf()
    .setAppName(getClass.getName)
    .setMaster("local[*]")
  val ssc = new StreamingContext(sc, Seconds(1))

  // stream of (topic, ImpressionLog)
  val messages = KafkaUtils.createDirectStream
    [String, AdImpressionLog, StringDecoder, AdImpressionLogAvroDecoder](ssc, kafkaParams, topics)

  // to count uniques
  lazy val hyperLogLog = new HyperLogLogMonoid(12)

  // we filter out non resolved geo (unknown) and map (pub, geo) -> AggLog that will be reduced
  val logsByPubGeo = messages.map(_._2).filter(_.getGeo != Configs.UnknownGeo).map {
    log =>
      val key = PublisherGeoKey(log.getPublisher.toString, log.getGeo.toString)
      val agg = AggregationLog(
        timestamp = log.getTimestamp,
        sumBids = log.getBid,
        imps = 1,
        uniquesHll = hyperLogLog(log.getCookie.toString.getBytes(Charsets.UTF_8))
      )
      (key, agg)
  }

  // Reduce to generate imps, uniques, sumBid per pub and geo per 2 seconds
  val aggLogs = logsByPubGeo.reduceByKeyAndWindow(reduceAggregationLogs, Seconds(2))

  aggLogs.foreachRDD(rdd => {
    rdd.foreach(f => {
      println("AggregationLog {timestamp=" + f._2.timestamp + " sumBids=" + f._2.sumBids + " imps=" + f._2.imps + "}")
    })
  })

  // start rolling!
  ssc.start
  ssc.awaitTermination

  private def reduceAggregationLogs(aggLog1: AggregationLog, aggLog2: AggregationLog) = {
    aggLog1.copy(
      timestamp = math.min(aggLog1.timestamp, aggLog2.timestamp),
      sumBids = aggLog1.sumBids + aggLog2.sumBids,
      imps = aggLog1.imps + aggLog2.imps,
      uniquesHll = aggLog1.uniquesHll + aggLog2.uniquesHll
    )
  }
}

case class AggregationLog(timestamp: Long, sumBids: Double, imps: Int = 1, uniquesHll: HLL)

case class AggregationResult(date: DateTime, publisher: String, geo: String, imps: Int, uniques: Int, avgBids: Double)

case class PublisherGeoKey(publisher: String, geo: String)
