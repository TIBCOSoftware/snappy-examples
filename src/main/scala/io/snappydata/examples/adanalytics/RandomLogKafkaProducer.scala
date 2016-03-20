package io.snappydata.examples.adanalytics

import java.util.Properties

import io.snappydata.examples.adanalytics.Constants._
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import scala.util.Random

object RandomLogKafkaProducer extends App {

  val props = new Properties()
  props.put("metadata.broker.list", "localhost:9092,localhost:9093")
  // props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
  // props.put("key.serializer.class", "kafka.serializer.StringEncoder")
  props.put("serializer.class", "io.snappydata.examples.adanalytics.AdImpressionLogAvroEncoder")
  // props.put("request.required.acks", "1")
  // props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  // props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  props.put("producer.type", "async")
  // props.put("batch.size", "8196")


    val config = new ProducerConfig(props)
    val producer = new Producer[String, AdImpressionLog](config)

    println("Sending messages...")
    val random = new Random()
    var i = 0
    // infinite loop
    while (true) {
      val timestamp = System.currentTimeMillis()
      val publisher = Publishers(random.nextInt(NumPublishers))
      val advertiser = Advertisers(random.nextInt(NumAdvertisers))
      val website = s"website_${random.nextInt(Constants.NumWebsites)}.com"
      val cookie = s"cookie_${random.nextInt(Constants.NumCookies)}"
      val geo = Geos(random.nextInt(Geos.size))
      val bid = math.abs(random.nextDouble()) % 1
      val log = new AdImpressionLog()
      log.setTimestamp(timestamp)
      log.setPublisher(publisher)
      log.setAdvertiser(advertiser)
      log.setWebsite(website)
      log.setGeo(geo)
      log.setBid(bid)
      log.setCookie(cookie)
      producer.send(new KeyedMessage[String, AdImpressionLog](Constants.kafkaTopic, log))
      i = i + 1
      if (i % 100000 == 0) {
        println(s"Sent $i messages!")
      }
    }
}


