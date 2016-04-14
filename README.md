SnappyData aims to deliver interactive speed analytics with modest investments in cluster infrastructure and far less complexity than today. SnappyData, fulfills this promise by 
- enabling streaming, transactions and interactive analytics in a single unifying system rather than stitching different solutions—and 
- delivering true interactive speeds via a state-of-the-art approximate query engine that can leverage a multitude of synopses as well as the full dataset. SnappyData implements this by deeply integrating an in-memory database into Spark. 

### Purpose
Here we use a simplified Ad Analytics example streaming in [AdImpression](https://en.wikipedia.org/wiki/Impression_(online_media)) logs, pre-aggregating the logs and ingesting into the built-in in-memory columnar store (where the data is stored both in 'exact' form as well as a stratified sample). 
We showcase the following aspects of this unified cluster:
- Simplicity of using SQL to model streams in spark. 
- using SQL (as continuous queries) to pre-aggregate AdImpression logs rather than map-reduce (it is faster and lot easier to incorporate more complex analytics).
- Highly efficient, yet simple way to ingest this pre-aggregated into the columnar store. While the store itself provides a rich set of features like hybrid row+column store,eager replication, WAN replicas, HA, choice of memory-only, HDFS, native disk persistence, eviction, etc we only work with a colum table in this simple example.
- Run OLAP queries from any SQL client both on the full data set as well as Sampled data (showcasing sub-second interactive query speeds). The stratified sample allows us to manage an infinitely growing data set at a fraction of the cost otherwise required.

### Ad Impression Analytics use case
We borrow our use case implementation from this [blog](https://chimpler.wordpress.com/2014/07/01/implementing-a-real-time-data-pipeline-with-spark-streaming/) - We more or less use the same data structure and aggregation logic but adapted this to showcase the SnappyData programming model extensions to Spark. We retain the native Spark example for comparison. 
Our architecture is depicted in the figure below. 
We consider an adnetwork where adservers log impressions in Apache Kafka (distributed publish-subscribe messaging system). These impressions are then aggregated by Spark Streaming into the SnappyData Store. External clients connect to the same cluster using JDBC/ODBC and run arbitrary OLAP queries. 
As AdServers could feed logs from many websites and given that each AdImpression log message represents a single Ad viewed by a user, you can expect thousands of messages every second. It is crucial that ingestion logic keeps up with the stream. To accomplish this, Snappydata colocates the store partitions with partitions created by Spark streaming. i.e. a batch of data from the stream in each Spark executor is transformed into a compressed column batch and stored in the same JVM, avoiding redundant shuffles(except for HA). 

The incoming AdImpression log is formatted as depicted below. 

|timestamp           |publisher |advertiser| website  |geo|bid    |cookie|
|--------------------|----------|----------|----------|---|-------|------|
|2013-01-28 13:21:12 |     pub1 |     adv10|   abc.com| NY| 0.0001|  1214|
|2013-01-28 13:21:13 |     pub1 |     adv10|   abc.com| NY| 0.0005|  1214|
|2013-01-28 13:21:14 |     pub2 |     adv20|   xyz.com| CA| 0.0003|  4321|
|2013-01-28 13:21:15 |     pub2 |     adv20|   xyz.com| CA| 0.0001|  5675|

We pre-aggregate these logs by publisher and geo, and compute the average bid, the number of impressions and the number of uniques(the number of unique users that viewed the Ad) every 2 seconds. We want to maintain the last day’s worth of data in memory for interactive queries. 
Some examples of interactive queries:  
- Find total uniques for a certain AD grouped on geography; 
- Impression trends for advertisers over time; 
- Top ads based on uniques count for each Geo. 

So the aggregation will look something like:

|timestamp           |publisher |geo    | avg_bid  |imps|uniques|
|--------------------|----------|-------|----------|----|-------|
|2013-01-28 13:21:00 |     pub1 |    NY |  0.0003  | 256| 104   |
|2013-01-28 13:21:00 |     pub2 |    CA |  0.0002  | 121| 15    |
|2013-01-28 13:22:00 |     pub1 |    NY |  0.0001  | 190| 98    |
|2013-01-28 13:22:00 |     pub2 |    CA |  0.0007  | 137| 19    |

### Let's get this going
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
From the root kafka folder, Create a topic “adImpressionsTopic”:
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --partitions 4 --topic adImpressionsTopic --replication-factor=1
```
Please download binary for SnppayData 0.2.1 Preview release from here : https://github.com/SnappyDataInc/snappydata/releases/tag/v0.2.1-preview
Unzip it to SnappyData-0.2.1-PREVIEW
TODO: CORRECT THE PRODUCT VERSION AND DOWNLOAD LOCATION
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
We can even verify if the data is getting stored in the aggrAdImpressions column table by using snappy-shell.
Please make sure your Spark Driver program is running while connecting to snappy-shell. 
```
SnappyData-0.2.1-PREVIEW/bin $ ./snappy-shell 
SnappyData version 2.0-BETA
snappy> connect client 'localhost:1527';
Using CONNECTION0
snappy> select count(*) from aggrAdImpressions;
c0                 
--------------------
134510 
```

