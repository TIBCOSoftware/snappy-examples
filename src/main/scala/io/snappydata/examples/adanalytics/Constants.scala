package io.snappydata.examples.adanalytics

import org.apache.spark.streaming.Seconds

object Constants {
  val NumPublishers = 50
  val NumAdvertisers = 30

  val Publishers = (0 to NumPublishers).map("publisher_" +)
  val Advertisers = (0 to NumAdvertisers).map("advertiser_" +)
  val UnknownGeo = "unknown"
  val Geos = Seq("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM",
    "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
    "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", UnknownGeo)
  val NumWebsites = 10000
  val NumCookies = 10000

  val totalNumLogs = 30000000;

  val kafkaTopic = "adlogsTopic"

  val kafkaParams: Map[String, String] = Map(
    "metadata.broker.list" -> "localhost:9092"
  )

  val batchDuration = Seconds(1)

  val topics = Set(Constants.kafkaTopic)
}
