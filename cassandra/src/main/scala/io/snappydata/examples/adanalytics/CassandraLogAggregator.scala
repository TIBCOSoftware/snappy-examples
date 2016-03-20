package io.snappydata.examples.adanalytics

import com.datastax.spark.connector.cql.CassandraConnector
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector._

object CassandraLogAggregator extends App {

  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.cassandra.auth.username", "cassandra")
    .set("spark.cassandra.auth.password", "cassandra")
  // val sc = new SparkContext("spark://127.0.0.1:7077", "test", conf)
  // val sc = new SparkContext("spark://127.0.0.1:7100", "test", conf)
  val sc = new SparkContext("local[4]", "test", conf)

  val keySpace = "logs"
  val tableName = "impressions'"

  CassandraConnector(conf).withSessionDo { session =>
    session.execute(s"DROP KEYSPACE IF EXISTS logs")
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS logs WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(s"CREATE TABLE IF NOT EXISTS logs.impressions (timestamp bigint primary key, publisher text, advertiser text, " +
      "website text, geo text, bid double, cookie text)")
  }

//  CREATE KEYSPACE logs WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;
//
//  CREATE TABLE logs.impressions (
//    timestamp bigint PRIMARY KEY,
//    advertiser text,
//    bid double,
//    cookie text,
//    geo text,
//    publisher text,
//    website text
//  ) WITH bloom_filter_fp_chance = 0.01
//  AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
//  AND comment = ''
//  AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
//  AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
//  AND crc_check_chance = 1.0
//  AND dclocal_read_repair_chance = 0.1
//  AND default_time_to_live = 0
//  AND gc_grace_seconds = 864000
//  AND max_index_interval = 2048
//  AND memtable_flush_period_in_ms = 0
//  AND min_index_interval = 128
//  AND read_repair_chance = 0.0
//  AND speculative_retry = '99PERCENTILE';


  val ssc = new StreamingContext(sc, Seconds(1))
  val kafkaParams: Map[String, String] = Map(
    "metadata.broker.list"->"localhost:9092,localhost:9093"
  )

  val topics  = Set(Constants.cassandraKafkaTopic)

  val messages = KafkaUtils.createDirectStream
    [String, AdImpressionLog, StringDecoder, AdImpressionLogAvroDecoder](ssc, kafkaParams, topics)

  messages.foreachRDD( rdd => rdd.map({case(topic, adImp) => (adImp.getTimestamp, adImp.getPublisher,
    adImp.getAdvertiser, adImp.getWebsite, adImp.getGeo, adImp.getBid, adImp.getCookie)})
    .saveToCassandra("logs", "impressions", SomeColumns("timestamp", "publisher", "advertiser", "website", "geo", "bid", "cookie"))
  )

  // start rolling!
  ssc.start
  ssc.awaitTermination

}
