In order to run this example, we need to install the followings:

1. Scala 2.10
2. Apache Kafka
3. SnappyData

Please follow the below steps to run the example:

Start Zookeeper:
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
Start Kafka Broker1:
```
bin/kafka-server-start.sh config/server1.properties
```
Start Kafka Broker2:
```
bin/kafka-server-start.sh config/server2.properties
```
Create a topic “adnetwork-topic”:
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic adnetwork-topic
```

Start generating and publishing logs to Kafka
```
./gradlew gen
```

Start SnappyData Locator:
In snappy-commons/build-artifacts/scala-2.10/snappy/conf, create a file named locators and add following line : 
```
localhost -peer-discovery-port=10334 -dir=/tmp/locator -heap-size=1024m 
```
and run following command 
```
snappy-commons/build-artifacts/scala-2.10/snappy/sbin $ ./snappy-locators.sh start
```

Start SnappyData Servers:  
In snappy-commons/build-artifacts/scala-2.10/snappy/conf, create a file named servers and add following two lines to create two servers: 
```
localhost -dir='/tmp/server1' -locators='localhost:10334' -heap-size=4096m -classpath='PATH_TO_GIT_CHECKOUT/ad_impression_log_aggregator/build/libs/LogAggregator-1.0-SNAPSHOT.jar'
localhost -dir='/tmp/server2' -locators='localhost:10334' -heap-size=4096m -classpath='PATH_TO_GIT_CHECKOUT/ad_impression_log_aggregator/build/libs/LogAggregator-1.0-SNAPSHOT.jar'
```
and run following command 
```
snappy-commons/build-artifacts/scala-2.10/snappy/sbin $ ./snappy-servers.sh start

```

Start aggregation
```
./gradlew aggr
```
