
##### [Skip directly to instructions](#lets-get-this-going)


### Table of Contents
1. [Introduction](#introduction)
2. [Purpose](#purpose)
3. [Ad Analytics use case](#ad-impression-analytics-use-case)
4. [Code highlights](#code-highlights)
5. [Just want to get it running](#lets-get-this-going)
6. [Interact with the data](#next-interact-with-the-data-fast)
7. [Slack/Gitter/Stackoverflow discussion](#ask-questions-start-a-discussion)

### Introduction
[SnappyData](https://github.com/SnappyDataInc/snappydata) aims to deliver real time operational analytics at interactive speeds with commodity infrastructure and far less complexity than today. SnappyData fulfills this promise by
- Enabling streaming, transactions and interactive analytics in a single unifying system rather than stitching different solutions—and
- Delivering true interactive speeds via a state-of-the-art approximate query engine that leverages a multitude of synopses as well as the full dataset. SnappyData implements this by deeply integrating an in-memory database into Apache Spark.

### Purpose
Here we use a simplified Ad Analytics example, which streams in [AdImpression](https://en.wikipedia.org/wiki/Impression_(online_media)) logs, pre-aggregating the logs and ingesting into the built-in in-memory columnar store (where the data is stored both in 'exact' form as well as a stratified sample).
We showcase the following aspects of this unified cluster:
- Simplicity of using the [DataFrame API](http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes) to model streams in spark.
- The use of Structured Streaming API to pre-aggregate AdImpression logs (it is faster and much more convenient to incorporate more complex analytics, rather than using map-reduce).
- Demonstrate storing the pre-aggregated logs into the SnappyData columnar store with high efficiency. While the store itself provides a rich set of features like hybrid row+column store, eager replication, WAN replicas, HA, choice of memory-only, HDFS, native disk persistence, eviction, etc we only work with a column table in this simple example.
- Run OLAP queries from any SQL client both on the full data set as well as sampled data (showcasing sub-second interactive query speeds). The stratified sample allows us to manage an infinitely growing data set at a fraction of the cost otherwise required.

### Ad Impression Analytics use case
We borrow our use case implementation from this [blog](https://chimpler.wordpress.com/2014/07/01/implementing-a-real-time-data-pipeline-with-spark-streaming/) - We more or less use the same data structure and aggregation logic and we have adapted this code to showcase the SnappyData programming model extensions to Spark. We retain the native Spark example for comparison.

Our architecture is depicted in the figure below.

We consider an adnetwork where adservers log impressions in [Apache Kafka](http://kafka.apache.org/) (distributed publish-subscribe messaging system). These impressions are then aggregated by [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) into the SnappyData Store. External clients connect to the same cluster using JDBC/ODBC and run arbitrary OLAP queries.
As AdServers can feed logs from many websites and given that each AdImpression log message represents a single Ad viewed by a user, one can expect thousands of messages every second. It is crucial that ingestion logic keeps up with the stream. To accomplish this, SnappyData collocates the store partitions with partitions created by Spark Streaming. i.e. a batch of data from the stream in each Spark executor is transformed into a compressed column batch and stored in the same JVM, avoiding redundant shuffles (except for HA).

TODO: Need to review this architecture diagram
![Architecture Kinda](AdAnalytics_Architecture.png)


The incoming AdImpression log is formatted as depicted below.

|timestamp              |publisher  |advertiser  | website  |geo|bid     |cookie   |
|-----------------------|-----------|------------|----------|---|--------|---------|
|2016-05-25 16:45:29.027|publisher44|advertiser11|website233|NJ |0.857122|cookie210|                           
|2016-05-25 16:45:29.027|publisher31|advertiser18|website642|WV |0.211305|cookie985|                           
|2016-05-25 16:45:29.027|publisher21|advertiser27|website966|ND |0.539119|cookie923|                           
|2016-05-25 16:45:29.027|publisher34|advertiser11|website284|WV |0.050856|cookie416|                           
|2016-05-25 16:45:29.027|publisher29|advertiser29|website836|WA |0.896101|cookie781|                           


We pre-aggregate these logs by publisher and geo, and compute the average bid, the number of impressions and the number of uniques (the number of unique users that viewed the Ad) every 2 seconds. We want to maintain the last day’s worth of data in memory for interactive analytics from external clients.
Some examples of interactive queries:
- **Find total uniques for a certain AD grouped on geography;**
- **Impression trends for advertisers over time;**
- **Top ads based on uniques count for each Geo.**

//todo[vatsal]: the timestamp millis values should be zero if we are aggregating on the window of 1 second
So the aggregation will look something like:
    
|timestamp               |publisher  |geo | avg_bid          |imps|uniques|
|------------------------|-----------|----|------------------|----|-------|
|2016-05-25 16:45:01.026 |publisher10| UT |0.5725387931435979|30  |26     |              
|2016-05-25 16:44:56.21  |publisher43| VA |0.5682680168342149|22  |20     |              
|2016-05-25 16:44:59.024 |publisher19| OH |0.5619481767564926|5   |5      |             
|2016-05-25 16:44:52.985 |publisher11| VA |0.4920346523303594|28  |21     |              
|2016-05-25 16:44:56.803 |publisher38| WI |0.4585381957119518|40  |31     |

### Code highlights
We implemented the ingestion logic using 3 methods mentioned below but only describe the SQL approach for brevity here.
TODO: verify the links
- [Vanilla Spark API](https://github.com/SnappyDataInc/snappy-poc/blob/master/src/main/scala/io/snappydata/adanalytics/SparkLogAggregator.scala)
- [Spark API with Snappy extensions](https://github.com/SnappyDataInc/snappy-poc/blob/master/src/main/scala/io/snappydata/adanalytics/SnappyAPILogAggregator.scala) to work with the stream as a sequence of DataFrames.


#### Generating the AdImpression logs 
TODO: verify this step with along with the code snippet   
A [KafkaAdImpressionGenerator](src/main/scala/io/snappydata/adanalytics/KafkaAdImpressionProducer.scala) simulates Adservers and generates random [AdImpressionLogs](src/avro/adimpressionlog.avsc)(Avro formatted objects) in batches to Kafka.
  ```scala
  val props = new Properties()
  //props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "io.snappydata.adanalytics.AdImpressionLogAvroSerializer")
  props.put("bootstrap.servers", "localhost:9092")

  val producer = new KafkaProducer[String, AdImpressionLog](props)

  def main(args: Array[String]) {
    println("Sending Kafka messages of topic " + kafkaTopic + " to brokers " + brokerList)
    val threads = new Array[Thread](numProducerThreads)
    for (i <- 0 until numProducerThreads) {
      val thread = new Thread(new Worker())
      thread.start()
      threads(i) = thread
    }
    threads.foreach(_.join())
    println(s"Done sending $numLogsPerThread Kafka messages of topic $kafkaTopic")
    System.exit(0)
  }

  def sendToKafka(log: AdImpressionLog): Future[RecordMetadata] = {
    producer.send(new ProducerRecord[String, AdImpressionLog](
      Configs.kafkaTopic, log.getTimestamp.toString, log))
  }
  ```
#### Spark Structured Streaming With Snappysink
 [SnappyLogAggregator](src/main/scala/io/snappydata/adanalytics/SnappyLogAggregatorJob.scala) creates a stream over the Kafka source and ingests data into Snappydata table using [`Snappysink`](https://snappydatainc.github.io/snappydata/howto/use_stream_processing_with_snappydata/#structured-streaming).

TODO: replace with structured streaming code
```scala
  val sc = new SparkContext(sparkConf)
  val snsc = new SnappyStreamingContext(sc, batchDuration)

  /**
  * AdImpressionStream presents the stream as a Table. It is registered with the Snappy catalog and hence queriable.
  * Underneath the covers, this is an abstraction over a DStream. DStream batches are emitted as DataFrames here.
  */
  snsc.sql("create stream table adImpressionStream (" +
    " time_stamp timestamp," +
    " publisher string," +
    " advertiser string," +
    " website string," +
    " geo string," +
    " bid double," +
    " cookie string) " +
    " using directkafka_stream options" +
    " (storagelevel 'MEMORY_AND_DISK_SER_2'," +
    " rowConverter 'io.snappydata.adanalytics.AdImpressionToRowsConverter' ," +
    s" kafkaParams 'metadata.broker.list->$brokerList'," +
    s" topics '$kafkaTopic'," +
    " K 'java.lang.String'," +
    " V 'io.snappydata.adanalytics.AdImpressionLog', " +
    " KD 'kafka.serializer.StringDecoder', " +
    " VD 'io.snappydata.adanalytics.AdImpressionLogAvroDecoder')")
    
    // Aggregate metrics for each publisher, geo every few seconds. Just 1 second in this example.
    // With the stream registered as a table, we can execute arbitrary queries.
    // These queries run each time a batch is emitted by the stream. A continuous query.
    val resultStream: SchemaDStream = snsc.registerCQ(
      "select min(time_stamp), publisher, geo, avg(bid) as avg_bid," +
        " count(*) as imps , count(distinct(cookie)) as uniques" +
        " from adImpressionStream window (duration 1 seconds, slide 1 seconds)" +
        " where geo != 'unknown' group by publisher, geo")
```

#### Ingesting into a Sample table
Finally, create a sample table that ingests from the column table specified above. This is the table that approximate queries will execute over. Here we create a query column set on the 'geo' column, specify how large of a sample we want relative to the column table (3%) and specify which table to ingest from:

```scala
  snsc.sql("CREATE SAMPLE TABLE sampledAdImpressions" +
    " OPTIONS(qcs 'geo', fraction '0.03', strataReservoirSize '50', baseTable 'aggrAdImpressions')")
```

### Let's get this going
In order to run this example, we need to install the following:

1. [Apache Kafka 2.11-0.10.2.2](https://archive.apache.org/dist/kafka/0.10.2.2/kafka_2.11-0.10.2.2.tgz)  
TODO: link to enterprise TIBCO ComputeDB here?
2. [SnappyData 1.1.1 Enterprise Release](). Download the binary snappydata-1.0.0-bin.tar.gz and Unzip it.
The binaries will be inside "snappydata-1.1.1-bin" directory.
3. JDK 8

Then checkout the Ad analytics example
```
git clone https://github.com/SnappyDataInc/snappy-poc.git
```

Note that the instructions for kafka configuration below are for 2.11-0.10.2.2 version of Kafka.

To setup kafka cluster, start Zookeeper first from the root kafka folder with default zookeeper.properties:
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Start one Kafka broker with default properties:
```
bin/kafka-server-start.sh config/server.properties
```

From the root kafka folder, Create a topic "adImpressionsTopic":
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --partitions 8 --topic adImpressionsTopic --replication-factor=1
```

Next from the checkout `/snappy-poc/` directory, build the example
```
-- Build and create a jar having all dependencies in assembly/build/libs
./gradlew assemble

-- If you use IntelliJ as your IDE, you can generate the project files using
./gradlew idea    (Try ./gradlew tasks for a list of all available tasks)
```

Goto the SnappyData product install home directory.
In conf subdirectory, create file "spark-env.sh"(copy spark-env.sh.template) and add this line ...

```
SPARK_DIST_CLASSPATH=SNAPPY_POC_HOME/assembly/build/libs/snappy-poc-1.1.1-assembly.jar
```
> Make sure you set the SNAPPY_POC_HOME directory appropriately above

Leave this file open as you will copy/paste the path for SNAPPY_POC_HOME shortly.

Start SnappyData cluster using following command from installation directory. 

```
./sbin/snappy-start-all.sh
```

This will start one locator, one server and a lead node. You can understand the roles of these nodes [here](https://github.com/SnappyDataInc/snappydata/blob/master/docs/GettingStarted.md#snappydata-cluster-explanation)

Submit the streaming job to the cluster and start it (consume the stream, aggregate and store).
> Make sure you copy/paste the SNAPPY_POC_HOME path from above in the command below where indicated

```
./bin/snappy-job.sh submit --lead localhost:8090 --app-name AdAnalytics --class io.snappydata.adanalytics.SnappyLogAggregatorJob --app-jar SNAPPY_POC_HOME/assembly/build/libs/snappy-poc-1.1.1-assembly.jar
```

SnappyData supports "Managed Spark Drivers" by running these in Lead nodes. So, if the driver were to fail, it can automatically re-start on a standby node. While the Lead node starts the streaming job, the actual work of parallel processing from kafka, etc is done in the SnappyData servers. Servers execute Spark Executors collocated with the data. 

Start generating and publishing logs to Kafka from the `/snappy-poc/` folder
```
./gradlew generateAdImpressions
```
TODO: provide some instruction to verify that the streaming is running without delay.
You can see the Spark streaming processing batches of data once every second in the [Spark console](http://localhost:4040/streaming/). It is important that our stream processing keeps up with the input rate. So, we note that the 'Scheduling Delay' doesn't keep increasing and 'Processing time' remains less than a second.

### Next, interact with the data. Fast.
Now, we can run some interactive analytic queries on the pre-aggregated data. From the root SnappyData folder, enter:

```
./bin/snappy-shell
```

Once this loads, connect to your running local cluster with:

```
connect client 'localhost:1527';
```

Set Spark shuffle partitions low since we don't have a lot of data; you can optionally view the members of the cluster as well:

```
set spark.sql.shuffle.partitions=7;
show members;
```

Let's do a quick count to make sure we have the ingested data:

```sql
select count(*) from aggrAdImpressions;
```

Now, lets run some OLAP queries on the column table of exact data. First, lets find the top 20 geographies with the most ad impressions:

```sql
select count(*) AS adCount, geo from aggrAdImpressions group by geo order by adCount desc limit 20;
```

Next, let's find the total uniques for a given ad, grouped by geography:

```sql
select sum(uniques) AS totalUniques, geo from aggrAdImpressions where publisher='publisher11' group by geo order by totalUniques desc limit 20;
```

Now that we've seen some standard OLAP queries over the exact data, let's execute the same queries on our sample tables using SnappyData's [Approximate Query Processing techinques](https://github.com/SnappyDataInc/snappydata/blob/master/docs/aqp.md). In most production situations, the latency difference here would be significant because the volume of data in the exact table would be much higher than the sample tables. Since this is an example, there will not be a significant difference; we are showcasing how easy AQP is to use.

We are asking for an error rate of 20% or below and a confidence interval of 0.95 (note the last two clauses on the query). The addition of these last two clauses route the query to the sample table despite the exact table being in the FROM clause. If the error rate exceeds 20% an exception will be produced:

```sql
select count(*) AS adCount, geo from aggrAdImpressions group by geo order by adCount desc limit 20 with error 0.20 confidence 0.95 ;
```

And the second query from above:

```sql
select sum(uniques) AS totalUniques, geo from aggrAdImpressions where publisher='publisher11' group by geo order by totalUniques desc limit 20 with error 0.20 confidence 0.95 ;
```

Note that you can still query the sample table without specifying error and confidence clauses by simply specifying the sample table in the FROM clause:

```sql
select sum(uniques) AS totalUniques, geo from sampledAdImpressions where publisher='publisher11' group by geo order by totalUniques desc;
```

Now, we check the size of the sample table:

```sql
select count(*) as sample_cnt from sampledAdImpressions;
```

Finally, stop the SnappyData cluser with:

```
./sbin/snappy-stop-all.sh
```

### So, what was the point again?
Hopefully we showed you how simple yet flexible it is to parallely ingest, process using SQL, run continuous queries, store data in column and sample tables and interactively query data. All in a single unified cluster. 
We will soon release Part B of this exercise - a benchmark of this use case where we compare SnappyData to other alternatives. Coming soon. 

### Ask questions, start a Discussion

[Stackoverflow](http://stackoverflow.com/questions/tagged/snappydata) ![Stackoverflow](http://i.imgur.com/LPIdp12.png)    [Slack](http://snappydata-slackin.herokuapp.com/)![Slack](http://i.imgur.com/h3sc6GM.png)        [Gitter](https://gitter.im/SnappyDataInc/snappydata) ![Gitter](http://i.imgur.com/jNAJeOn.jpg)          [Mailing List](https://groups.google.com/forum/#!forum/snappydata-user) ![Mailing List](http://i.imgur.com/NUsavRg.png)             [Reddit](https://www.reddit.com/r/snappydata) ![Reddit](http://i.imgur.com/AB3cVtj.png)          [JIRA](https://jira.snappydata.io/projects/SNAP/issues) ![JIRA](http://i.imgur.com/E92zntA.png)

### Source code, docs
[SnappyData Source](https://github.com/SnappyDataInc/snappydata)

[SnappyData Docs](http://snappydatainc.github.io/snappydata/)

[This Example Source](https://github.com/SnappyDataInc/snappy-poc)

[SnappyData Technical Paper](http://www.snappydata.io/snappy-industrial)


