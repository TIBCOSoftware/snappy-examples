Serving an ad involves multiple information pieces stored in various repositories, starting with the user profile, the appropriate real time bids based on the profile, ad networks that can serve those ads efficiently and often involves working with a different systems/clusters or tools for each activity. These clusters often involve redundant data management, tuning, debugging and constitute individual points of failure in a business that is extremely sensitive to latency and delays.

Our example will showcase the way to do this analysis fast and also make sure the output gets ingested into a data store with high throughput. Moreover, the data, once ingested, needs to be interactively accessed using analytic queries. Our intent is to showcase a single SnappyData cluster that can:
- Keep up with the stream, 
- Run continuous queries (stream analytics) and 
- Store the output in memory so client applications can run interactive queries concurrently with the stream. 

By running aggregates on the incoming micro-batches and storing those aggregations with minimal network hops so as to not throttle the incoming stream, running OLAP queries on the stored aggregates can occur interactively. A typical ad impression record comprises fields like publisher, advertiser, website, geo, bid, cookie, and timestamp. In this case, we chose to group these records by Geo and Publisher and store aggregations into the data store.

As mentioned in this blog - https://chimpler.wordpress.com/2014/07/01/implementing-a-real-time-data-pipeline-with-spark-streaming/
we will take the same format ad for impression logs:

|timestamp           |publisher |advertiser| website  |geo|bid    |cookie|
|--------------------|----------|----------|----------|---|-------|------|
|2013-01-28 13:21:12 |     pub1 |    adv10 |   abc.com| NY| 0.0001|  1214|
|2013-01-28 13:21:13 |     pub1 |     adv10|   abc.com| NY| 0.0005|  1214|
|2013-01-28 13:21:14 |     pub2 |    adv20 |   xyz.com| CA| 0.0003|  4321|
|2013-01-28 13:21:15 |     pub2 |     adv20|   xyz.com| CA| 0.0001|  5675|

We pre-aggregate these logs by publisher and geo, and compute the average bid, the number of impressions and the number of uniques by minute. We want to maintain the last day’s worth of data in memory for interactive queries. 
Some examples of interactive queries:  
- Find total uniques for a certain AD grouped on geography; 
- Impression trends for advertisers over time; 
- Top ads based on uniques count for each Geo. 

So the aggregation will look something like:

|timestamp           |publisher |geo    | avg_bid  |imps|uniques|
|--------------------|----------|-------|----------|----|-------|
|2013-01-28 13:21:00 |     pub1 |    NY |  0.0003  | 256| 104   |
|2013-01-28 13:21:00 |     pub2 |    CA |   0.0002 | 121| 15    |
|2013-01-28 13:22:00 |     pub1 |    NY |  0.0001  | 190| 98    |
|2013-01-28 13:22:00 |     pub2 |    CA |   0.0007 | 137| 19    |

In order to run this example, we need to install the followings:

1. Scala 2.10 or 2.11
2. [Apache Kafka 0.8.2.2 -> 0.9.0.1](http://kafka.apache.org/downloads.html)
3. [SnappyData 0.2.1 Preview Release](https://github.com/SnappyDataInc/snappydata/releases)
4. Gradle 2.11
5. JDK 7.0 or JDK 8

Please follow the below steps to run the example:

Then checkout the adanalytics example
```
git clone https://github.com/SnappyDataInc/snappy-poc.git
```
build the repo from the `/snappy-poc/` directory
```
./gradlew assemble
```
If you would like to use Intellij Idea, please run following build target
```
./gradlew idea 
```
And import the project in Idea.

Start Zookeeper from the root kafka folder with default zookeeper.properties:
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```

You can configure multiple Kafka brokers by simply having multiple copies of the /kafkaroot/config/server.properties file.
For example, duplicate server.properties as server1.properties & server2.properties

You need to then specify different broker.id, log.dir and port in each of those files to make them unique. Note that the port needs to be changed twice
```
   config/server1.properties:
     broker.id=1
     port=9092
     log.dir=/tmp/kafka-logs-1
```
```
   config/server2.properties:
     broker.id=2
     listeners=PLAINTEXT://:9093
     port=9093
     log.dir=/tmp/kafka-logs-2
```
And then start multiple instances from the root kafka folder with following commands

```
bin/kafka-server-start.sh config/server1.properties
bin/kafka-server-start.sh config/server2.properties
```
From the root kafka folder, Create a topic “adnetwork-topic”:
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --partitions 8 --topic adnetwork-topic --replication-factor=1
```
Please download binary for SnppayData 0.2.1 Preview release from here : https://github.com/SnappyDataInc/snappydata/releases/tag/v0.2.1-preview
Unzip it to SnappyData-0.2.1-PREVIEW

Start SnappyData Locator:
run following command from the root SnappyData directory
```
SnappyData-0.2.1-PREVIEW/sbin $ ./snappy-locators.sh start
```

Start SnappyData Servers:  
In SnappyData-0.2.1-PREVIEW/conf, create a file named servers and add following two lines to create two servers *make sure you add the absolute path*: 
```
localhost -classpath='absolute_path_to_snappy-poc-checkout/build/libs/AdImpressionLogAggr-2.0-SNAPSHOT.jar'
localhost -classpath='absolute_path_to_snappy-poc-checkout/build/libs/AdImpressionLogAggr-2.0-SNAPSHOT.jar'
```
and run following command

```
SnappyData-0.2.1-PREVIEW/sbin $ ./snappy-servers.sh start
```

Start aggregation from the `/snappy-poc/` folder
```
./gradlew aggeregateAdImpressions_SQL
```

Start generating and publishing logs to Kafka from the `/snappy-poc/` folder
```
./gradlew generateAdImpressions
```
We can even verify if the data is getting stored in the adImpressions column table by using snappy-shell. 
```
SnappyData-0.2.1-PREVIEW/bin $ ./snappy-shell 
SnappyData version 2.0-BETA
snappy> connect client 'localhost:1527';
Using CONNECTION0
snappy> select count(*) from adImpressions;
c0                 
--------------------
134510 
```

