In this post we are goinig to describe a use case mentioned in this blog: https://chimpler.wordpress.com/2014/07/01/implementing-a-real-time-data-pipeline-with-spark-streaming/
Where we consider an adnetwork where adservers log impressions in Apache Kafka. These impressions are then aggregated by Spark Streaming into an in-memory store from SnappyData. 
These ad impressions can provide direct lookup for fast data access, or can be sampled so that you can run error bouded queries which can return aproximate answers.
All this is achived in a sinle SnappyData cluster very easily and efficiently. 

In order to run this example, we need to install the followings:

1. Scala 2.10 or 2.11
2. [Apache Kafka 0.8.2.2 -> 0.9.0.1](http://kafka.apache.org/downloads.html)
3. [SnappyData 0.2.1 Preview Release](https://github.com/SnappyDataInc/snappydata/releases)
4. Gradle 2.11
5. JDK 7.0 or JDK 8

Please follow the below steps to run the example:

Then checkout the adanalytics example
```
git clone https://github.com/SnappyDataInc/snappy-examples.git
```
build the repo from the `/snappy-examples/` directory
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
localhost -classpath='absolute_path_to_snappy-examples-checkout/build/libs/AdImpressionLogAggr-2.0-SNAPSHOT.jar'
localhost -classpath='absolute_path_to_snappy-examples-checkout/build/libs/AdImpressionLogAggr-2.0-SNAPSHOT.jar'
```
and run following command

```
SnappyData-0.2.1-PREVIEW/sbin $ ./snappy-servers.sh start
```

Start generating and publishing logs to Kafka from the `/snappy-examples/` folder
```
./gradlew createAndPublishLogs
```

Start aggregation from the `/snappy-examples/` folder
```
./gradlew startLogAggregation
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




