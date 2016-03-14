In order to run this example, we need to install the followings:

1. Scala 2.10
2. Apache Kafka 0.8.2.2
3. SnappyData 0.2 Preview Release
4. Gradle 2.11
5. JDK 7.0

Please follow the below steps to run the example:

You need to checkout SnappyData product repo 
```
git clone https://github.com/SnappyDataInc/snappydata.git
```
To build the repo
```
./gradlew assemble
```
publish it locally 
```
./gradlew localPublish
```

Then checkout the adanalytics example
```
git clone https://github.com/SnappyDataInc/snappy-examples.git
```
build the repo
```
./gradlew assemble
```
If you would like to use Intellij Idea, please run following build target
```
./gradlew idea 
```
And import the project in Idea.

Start Zookeeper with default zookeeper.properties:
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
Start Kafka broker1 and broker2 in two different terminals on ports 9092 and 9093 respectively. You can create two copies of config/server.propoerties and edit ports to use 9092 and 9093 respectively.

```
bin/kafka-server-start.sh config/server1.properties
bin/kafka-server-start.sh config/server2.properties
```
Create a topic “adnetwork-topic”:
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic adnetwork-topic
```
Start SnappyData Locator:
run following command 
```
snappy-commons/build-artifacts/scala-2.10/snappy/sbin $ ./snappy-locators.sh start
```

Start SnappyData Servers:  
In snappy-commons/build-artifacts/scala-2.10/snappy/conf, create a file named servers and add following two lines to create two servers: 
```
localhost -classpath='snappy-examples/build/libs/LogAggregator-1.0-SNAPSHOT.jar'
localhost -classpath='snappy-examples/build/libs/LogAggregator-1.0-SNAPSHOT.jar'
```
and run following command 

```
snappy-commons/build-artifacts/scala-2.10/snappy/sbin $ ./snappy-servers.sh start
```

Start generating and publishing logs to Kafka
```
./gradlew gen
```

Start aggregation
```
./gradlew aggr
```
