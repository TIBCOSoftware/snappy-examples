package io.snappydata.examples.adanalytics

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CassandraLogAggregator extends App {

  val sc = new SparkConf()
    .setAppName("CassandraLogAggregator")
    .setMaster("local[4]")
  val ssc = new StreamingContext(sc, Seconds(1))
  val kafkaParams: Map[String, String] = Map(
    "metadata.broker.list"->"localhost:9092,localhost:9093"
  )

  val topics  = Set(Constants.KafkaTopic)

  // stream of (topic, ImpressionLog)
  val messages = KafkaUtils.createDirectStream
    [String, AdImpressionLog, StringDecoder, AdImpressionLogAvroDecoder](ssc, kafkaParams, topics)

  import com.datastax.spark.connector.streaming._
  messages.saveToCassandra("cassandraTable", "aggr" )

  // start rolling!
  ssc.start
  ssc.awaitTermination

}
