package io.snappydata.adanalytics.aggregator

import com.twitter.algebird.HyperLogLogMonoid
import io.snappydata.adanalytics.aggregator.Constants._
import kafka.serializer.StringDecoder
import org.apache.commons.io.Charsets
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
  val logsByPubGeo = messages.map(_._2).filter(_.getGeo != Constants.UnknownGeo).map {
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
