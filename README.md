
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
[SnappyData](https://github.com/SnappyDataInc/snappydata) aims to deliver real time operational analytics at interactive
speeds with commodity infrastructure and far less complexity than today.
SnappyData fulfills this promise by
- Enabling streaming, transactions and interactive analytics in a single unifying system rather than stitching different
solutions
- Delivering true interactive speeds via a state-of-the-art approximate query engine that leverages a multitude of
synopses as well as the full dataset. SnappyData implements this by deeply integrating an in-memory database into
Apache Spark.

### Purpose
Here we use a simplified Ad Analytics example, which streams in [AdImpression](https://en.wikipedia.org/wiki/Impression_(online_media))
logs, pre-aggregating the logs and ingesting into the built-in in-memory columnar store (where the data is stored both
in 'exact' form as well as a stratified sample).
We showcase the following aspects of this unified cluster:
- Simplicity of using the [DataFrame API](http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes) to model streams in Apache Spark.
- The use of Structured Streaming API to pre-aggregate AdImpression logs (it is faster and much more convenient to incorporate more complex analytics, rather than using map-reduce).
- Demonstrate storing the pre-aggregated logs into the SnappyData columnar store with high efficiency. While the store itself provides a rich set of features like hybrid row+column store, eager replication, WAN replicas, high-availability, choice of memory-only, HDFS and native disk persistence, eviction, etc, We only work with a column table in this simple example.
- Run OLAP queries from any SQL client both on the full data set as well as sampled data (showcasing sub-second interactive query speeds). The stratified sample allows us to manage an infinitely growing data set at a fraction of the cost otherwise required.

### Ad Impression Analytics use case
We borrow our use case implementation from this [blog](https://chimpler.wordpress.com/2014/07/01/implementing-a-real-time-data-pipeline-with-spark-streaming/)
\- We more or less use the same data structure and aggregation logic and we have adapted this code to showcase the
SnappyData programming model extensions to Spark. For comparison, we are also having the [native Spark example](src/main/scala/io/snappydata/adanalytics/SparkLogAggregator.scala) using structured streaming.

Our architecture is depicted in the figure below.

We consider an adnetwork where AdServers log impressions in [Apache Kafka](http://kafka.apache.org/). These impressions
are then aggregated using [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) into the SnappyData Store. External clients connect to the same cluster using JDBC/ODBC and run arbitrary OLAP queries.
As AdServers can feed logs from many websites and given that each AdImpression log message represents a single Ad viewed
by a user, one can expect thousands of messages every second. It is crucial that ingestion logic keeps up with the
stream. To accomplish this, SnappyData collocates the store partitions with partitions created by Spark Streaming.
i.e. a batch of data from the stream in each Spark executor is transformed into a compressed column batch and stored in
the same JVM, avoiding redundant shuffles (except for HA).

![Architecture Kinda](AdAnalytics_Architecture.png)


The incoming AdImpression log is formatted as depicted below.
                           
|timestamp              |publisher  |advertiser  |website   |geo|bid                |cookie   |
|-----------------------|-----------|------------|----------|---|-------------------|---------|
|2020-02-17 16:37:59.289|publisher24|advertiser5 |website478|NJ |0.6682117005884909 |cookie649|
|2020-02-17 16:37:59.289|publisher31|advertiser19|website337|NE |0.5697320252959912 |cookie340|
|2020-02-17 16:37:59.289|publisher27|advertiser14|website364|OK |0.2685715410844016 |cookie536|
|2020-02-17 16:37:59.289|publisher23|advertiser26|website531|MT |0.7226818935272965 |cookie487|
|2020-02-17 16:37:59.289|publisher3 |advertiser15|website937|MT |0.48053937420374915|cookie605|


We pre-aggregate these logs by publisher and geo, and compute the average bid, the number of impressions and the number
of uniques (the number of unique users that viewed the Ad) every second. We want to maintain the last day’s worth of
data in memory for interactive analytics from external clients.
Some examples of interactive queries:
- **Find total uniques for a certain AD grouped on geography**
- **Impression trends for advertisers over time**
- **Top ads based on uniques count for each Geo**

So the aggregation will look something like:

|time_stamp            |publisher  |geo|avg_bid            |imps|uniques|
|----------------------|-----------|---|-------------------|----|-------|
|2020-02-17 16:39:28.0 |publisher16|NY |0.4269814055107817 |190 |158    |
|2020-02-17 16:39:31.0 |publisher30|CT |0.4482890418617008 |19  |19     |
|2020-02-17 16:39:26.0 |publisher37|HI |0.21539768570303286|2   |2      |
|2020-02-17 16:39:33.0 |publisher38|ID |0.3639807522416625 |15  |15     |
|2020-02-17 16:39:27.0 |publisher37|OH |0.381703659839993  |25  |26     |

### Code highlights
We implemented the ingestion logic using [Vanilla Spark Structured Streaming](src/main/scala/io/snappydata/adanalytics/SparkLogAggregator.scala)
and [Spark Structured Streaming with Snappy Sink](src/main/scala/io/snappydata/adanalytics/SnappyLogAggregator.scala)
to work with the stream as a sequence of DataFrames.

#### Generating the AdImpression logs    
A [KafkaAdImpressionProducer](src/main/scala/io/snappydata/adanalytics/KafkaAdImpressionProducer.scala) simulates
Adservers and generates random [AdImpressionLogs](src/avro/adimpressionlog.avsc)(Avro formatted objects) in batches to Kafka.

```scala 
  val props = new Properties()
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "io.snappydata.adanalytics.AdImpressionLogAVROSerializer")
  props.put("bootstrap.servers", brokerList)

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
      Configs.kafkaTopic, log.getTimestamp.toString, log), new org.apache.kafka.clients.producer.Callback() {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          if (exception.isInstanceOf[RetriableException]) {
            println(s"Encountered a retriable exception while sending messages: $exception")
          } else {
            throw exception
          }
        }
      }
    }
    )
  }
```
#### Spark Structured Streaming With Snappysink
 [SnappyLogAggregator](src/main/scala/io/snappydata/adanalytics/SnappyLogAggregator.scala) creates a stream over the
 Kafka source and ingests data into Snappydata table using [`Snappysink`](https://snappydatainc.github.io/snappydata/howto/use_stream_processing_with_snappydata/#structured-streaming).

```scala

// The volumes are low. Optimize Spark shuffle by reducing the partition count
snappy.sql("set spark.sql.shuffle.partitions=8")

snappy.sql("drop table if exists aggrAdImpressions")

snappy.sql("create table aggrAdImpressions(time_stamp timestamp, publisher string," +
  " geo string, avg_bid double, imps long, uniques long) " +
  "using column options(buckets '11')")

val schema = StructType(Seq(StructField("timestamp", TimestampType), StructField("publisher",
  StringType), StructField("advertiser", StringType),
  StructField("website", StringType), StructField("geo", StringType),
  StructField("bid", DoubleType), StructField("cookie", StringType)))

import snappy.implicits._
val df = snappy.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", brokerList)
  .option("value.deserializer", classOf[ByteArrayDeserializer].getName)
  .option("startingOffsets", "earliest")
  .option("subscribe", kafkaTopic)
  .load()
  // projecting only value column of the Kafka data an using
  .select("value").as[Array[Byte]](Encoders.BINARY)
  .mapPartitions(itr => {
    // Reuse deserializer for each partition which will internally reuse decoder and data object
    val deserializer = new AdImpressionLogAVRODeserializer
    itr.map(data => {
      // deserializing AVRO binary data and formulating Row out of it
      val adImpressionLog = deserializer.deserialize(data)
      Row(new java.sql.Timestamp(adImpressionLog.getTimestamp), adImpressionLog.getPublisher
        .toString, adImpressionLog.getAdvertiser.toString, adImpressionLog.getWebsite.toString,
        adImpressionLog.getGeo.toString, adImpressionLog.getBid,
        adImpressionLog.getCookie.toString)
    })
  })(RowEncoder.apply(schema))
  // filtering invalid records
  .filter(s"geo != '${Configs.UnknownGeo}'")

// Aggregating records with
val windowedDF = df.withColumn("eventTime", $"timestamp".cast("timestamp"))
  .withWatermark("eventTime", "10 seconds")
  .groupBy(window($"eventTime", "1 seconds", "1 seconds"), $"publisher", $"geo")
  .agg(unix_timestamp(min("timestamp"), "MM-dd-yyyy HH:mm:ss").alias("timestamp"),
    avg("bid").alias("avg_bid"), count("geo").alias("imps"),
    approx_count_distinct("cookie").alias("uniques"))
  .select("timestamp", "publisher", "geo", "avg_bid", "imps", "uniques")

val logStream = windowedDF
  .writeStream
  .format("snappysink")               // using snappysink as output sink
  .queryName("log_aggregator")        // name of the streaming query
  .trigger(ProcessingTime("1 seconds"))     // trigger the batch processing every second
  .option("tableName", "aggrAdImpressions") // target table name where data will be ingested
  //checkpoint location where the streaming query progress and intermediate aggregation state
  // is stored. It should be ideally on some HDFS location.
  .option("checkpointLocation", snappyLogAggregatorCheckpointDir)
  // Only the rows that were updated since the last trigger will be outputted to the sink.
  // More details about output mode: https://spark.apache.org/docs/2.1.1/structured-streaming-programming-guide.html#output-modes
  .outputMode("update")
  .start
```

#### Ingesting into a Sample table
Finally, create a sample table that ingests from the column table specified above. This is the table that approximate
queries will execute over. Here we create a query column set on the 'geo' column, specify how large of a sample we want
relative to the column table (3%) and specify which table to ingest from:

```scala
snappy.sql("CREATE SAMPLE TABLE sampledAdImpressions" + 
" OPTIONS(qcs 'geo', fraction '0.03', strataReservoirSize '50', baseTable 'aggrAdImpressions')")
```

### Let's get this going
In order to run this example, we need to install the following:

1. [Apache Kafka 2.11-0.10.2.2](https://archive.apache.org/dist/kafka/0.10.2.2/kafka_2.11-0.10.2.2.tgz)
2. [TIBCO ComputeDB 1.2.0](https://tap.tibco.com/storefront/trialware/tibco-computedb-developer-edition/prod15349.html)
Or [Snappydata 1.2.0](https://github.com/SnappyDataInc/snappydata/releases/download/v1.2.0/snappydata-1.2.0-bin.tar.gz) 
3. JDK 8

To setup Kafka cluster, start Zookeeper first from the root Kafka folder with default zookeeper.properties:
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Start one Kafka broker with default properties:
```
bin/kafka-server-start.sh config/server.properties
```

From the root Kafka folder, Create a topic "adImpressionsTopic":
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --partitions 8 --topic adImpressionsTopic --replication-factor=1
```

Checkout the Ad analytics example
```
git clone https://github.com/SnappyDataInc/snappy-poc.git
```

Next from the checkout `/snappy-poc/` directory, build the example
```
-- Build and create a jar having all dependencies in assembly/build/libs
./gradlew assemble

-- If you use IntelliJ as your IDE, you can generate the project files using
./gradlew idea    (Try ./gradlew tasks for a list of all available tasks)
```

Goto the SnappyData product install home directory.
In conf subdirectory, create file "spark-env.sh" (copy spark-env.sh.template) and add this line ...

```
SPARK_DIST_CLASSPATH=<snappy_poc_home>/assembly/build/libs/snappy-poc-1.1.1-assembly.jar
```
> Make sure you set the `snappy_poc_home` directory appropriately above

Leave this file open as you will copy/paste the path for `snappy_poc_home` shortly.

Start SnappyData cluster using following command from installation directory. 

```
./sbin/snappy-start-all.sh
```

This will start one locator, one server and a lead node. You can understand the roles of these nodes [here](https://snappydatainc.github.io/snappydata/architecture/cluster_architecture/)

Submit the streaming job to the cluster and start it (consume the stream, aggregate and store).
> Make sure you copy/paste the SNAPPY_POC_HOME path from above in the command below where indicated

```
./bin/snappy-job.sh submit --lead localhost:8090 --app-name AdAnalytics --class io.snappydata.adanalytics.SnappyLogAggregator --app-jar $SNAPPY_POC_HOME/assembly/build/libs/snappy-poc-1.2.0-assembly.jar
```

8090 is the default port of spark-jobserver which is used to manage snappy jobs.  

SnappyData supports "Managed Spark Drivers" by running these in Lead nodes. So, if the driver were to fail, it can
automatically re-start on a standby node. While the Lead node starts the streaming job, the actual work of parallel
processing from Apache Kafka, etc is done in the SnappyData servers. Servers execute Spark Executors collocated with the data. 

Start generating and publishing logs to Kafka from the `/snappy-poc/` folder
```
./gradlew generateAdImpressions
```

You can monitor the streaming query progress on the [Structured Streaming UI](http://localhost:5050/structuredstreaming/). It is
important that our stream processing keeps up with the input rate. So, we should monitor that the `Processing Rate` keeps
up with `Input Rate` and `Processing Time` remains less than the trigger interval which is one second. 

### Next, interact with the data. Fast.
Now, we can run some interactive analytic queries on the pre-aggregated data. From the root SnappyData folder, enter:

```
./bin/snappy-shell
```

Once this loads, connect to your running local cluster with:

```
CONNECT CLIENT 'localhost:1527';
```

Set Spark shuffle partitions to a lower number since we don't have a lot of data; you can optionally view the members of the cluster
as well:

```
SET spark.sql.shuffle.partitions=8;
SHOW members;
```

Let's do a quick count to make sure we have the ingested data:

```sql
SELECT COUNT(*) FROM aggrAdImpressions;
```

Now, lets run some OLAP queries on the column table of exact data. First, lets find the top 20 geographies with the most
ad impressions:

```sql
SELECT COUNT(*) AS adCount, geo FROM aggrAdImpressions GROUP BY geo ORDER BY adCount DESC LIMIT 20;
```

Next, let's find the total uniques for a given ad, grouped by geography:

```sql
SELECT SUM(uniques) AS totalUniques, geo FROM aggrAdImpressions WHERE publisher='publisher11' GROUP BY geo ORDER BY totalUniques DESC LIMIT 20;
```

Now that we've seen some standard OLAP queries over the exact data, let's execute the same queries on our sample tables
using SnappyData's [Approximate Query Processing techinques](https://github.com/SnappyDataInc/snappydata/blob/master/docs/aqp.md).
In most production situations, the latency difference here would be significant because the volume of data in the exact
table would be much higher than the sample tables. Since this is an example, there will not be a significant difference;
we are showcasing how easy AQP is to use.

We are asking for an error rate of 20% or below and a confidence interval of 0.95 (note the last two clauses on the query).
The addition of these last two clauses route the query to the sample table despite the base table being in the FROM
clause. If the error rate exceeds 20% an exception will be produced:

```sql
SELECT COUNT(*) AS adCount, geo FROM aggrAdImpressions GROUP BY geo ORDER BY adCount DESC LIMIT 20 WITH ERROR 0.20 CONFIDENCE 0.95 ;
```

And the second query from above:

```sql
SELECT SUM(uniques) AS totalUniques, geo FROM aggrAdImpressions WHERE publisher='publisher11' GROUP BY geo ORDER BY totalUniques DESC LIMIT 20 WITH ERROR 0.20 CONFIDENCE 0.95 ;
```

Note that you can still query the sample table without specifying error and confidence clauses by simply specifying the
sample table in the FROM clause:

```sql
SELECT SUM(uniques) AS totalUniques, geo FROM sampledAdImpressions WHERE publisher='publisher11' GROUP BY geo ORDER BY totalUniques DESC;
```

Now, we check the size of the sample table:

```sql
SELECT COUNT(*) AS sample_cnt FROM sampledAdImpressions;
```

Finally, stop the SnappyData cluster with:

```
./sbin/snappy-stop-all.sh
```

### So, what was the point again?
Hopefully we showed you how simple yet flexible it is to parallely ingest, process using SQL, run continuous queries,
store data in column and sample tables and interactively query data. All in a single unified cluster. 

### Ask questions, start a Discussion

[Stackoverflow](http://stackoverflow.com/questions/tagged/snappydata) ![Stackoverflow](http://i.imgur.com/LPIdp12.png)    [Slack](http://snappydata-slackin.herokuapp.com/)![Slack](http://i.imgur.com/h3sc6GM.png)        [Gitter](https://gitter.im/SnappyDataInc/snappydata) ![Gitter](http://i.imgur.com/jNAJeOn.jpg)          [Mailing List](https://groups.google.com/forum/#!forum/snappydata-user) ![Mailing List](http://i.imgur.com/NUsavRg.png)             [Reddit](https://www.reddit.com/r/snappydata) ![Reddit](http://i.imgur.com/AB3cVtj.png)          [JIRA](https://jira.snappydata.io/projects/SNAP/issues) ![JIRA](http://i.imgur.com/E92zntA.png)

### Source code, docs
[SnappyData Source](https://github.com/SnappyDataInc/snappydata)

[SnappyData Docs](http://snappydatainc.github.io/snappydata/)

[This Example Source](https://github.com/SnappyDataInc/snappy-poc)

[SnappyData Technical Paper](http://www.snappydata.io/snappy-industrial)


