package io.snappydata.aggr


import com.twitter.algebird.HyperLogLogMonoid
import kafka.serializer.StringDecoder
import org.apache.commons.io.Charsets
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkLogAggregator extends App {

  val  batchDuration = Seconds(10)
  val sc = new SparkConf().setAppName("logAggregator").setMaster("local[4]")
  val ssc = new StreamingContext(sc, batchDuration)
  val kafkaParams: Map[String, String] = Map(
    "metadata.broker.list"->"localhost:9092,localhost:9093"
  )

  val topics  = Set(Constants.KafkaTopic)

  // stream of (topic, ImpressionLog)
  val messages = KafkaUtils.createDirectStream[String, ImpressionLog, StringDecoder, ImpressionLogAvroDecoder](ssc, kafkaParams, topics)

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

  // Reduce to generate imps, uniques, sumBid per pub and geo per interval of BatchDuration seconds
  val aggLogs = logsByPubGeo.reduceByKeyAndWindow(reduceAggregationLogs, batchDuration)

  aggLogs.foreachRDD(println(_,_))

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