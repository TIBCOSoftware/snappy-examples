package io.snappydata.examples.adanalytics

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

  val cassandraKafkaTopic = "cassandraAdNetwork"
  val memsqlaKafkaTopic = "memsqlAdNetwork"
  val kafkaTopic ="adnetwork-topic"

  // val KafkaTopic = "adnetwork-topic"
  // val KafkaTopic = "adLogs" // 1 partition  - 1L
  // val KafkaTopic = "adLogs1" // 8 partition 35K
  // val KafkaTopic = "adLogs2" // 1 partition 1L
  // val KafkaTopic = "adLogs3" // 1 partition fresh kafka cluster, 1L, only driver
  // val KafkaTopic = "adLogs4" // 1 partition snappy cluster, create stream table, 1L, table insertion lags
  // val KafkaTopic = "adLogs5" //1.3 minutes scheduleDelay
  // val KafkaTopic = "adLogs6"
  // val KafkaTopic = "adLogs7" // 8 partitions
  // val KafkaTopic ="cassandraTopic"

}
