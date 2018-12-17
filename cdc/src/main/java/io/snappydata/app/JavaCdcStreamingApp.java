/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.app;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SnappySession;
import org.apache.spark.sql.catalog.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.ProcessingTime;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.jdbc.StreamConf;
import org.apache.spark.util.Utils;
import scala.collection.Seq;

import static scala.collection.JavaConversions.seqAsJavaList;

public class JavaCdcStreamingApp {

  /**
   * A map of source and destination table. The source table should have been defined in
   * the source database and destination table in SnappyData
   */
  private final java.util.Map<String, String> sourceDestTables;

  private final java.util.Map<String, String> sourceOptions;
  Map<String, StreamingQuery> activeQueries = new ConcurrentHashMap<>();

//  private SnappySession snappySpark;

  public static void main(String[] args) throws Exception {
    JavaCdcStreamingApp _this = new JavaCdcStreamingApp(args);
    _this.startJob();
  }

  private JavaCdcStreamingApp(String[] args) throws Exception {
    sourceOptions = Collections.unmodifiableMap(fillSourceOptions(args));
    sourceDestTables = Collections.unmodifiableMap(configureTables(args));
  }

  private SnappySession createSnappySession() throws ClassNotFoundException, IOException {
    String checkPointDir = Utils.createTempDir(".", "stream-spark-cdc").getCanonicalPath();
    return new SnappySession(SparkSession.builder()
            .config("spark.sql.streaming.checkpointLocation", checkPointDir)
            .getOrCreate().sparkContext());
  }

  private void startJob() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    System.out.println("SourceOptiona are " + sourceOptions);

//    snappySpark = createSnappySession();
//
//    snappySpark.sparkContext().addSparkListener(new SparkContextListener(activeQueries));

    ExecutorService service = Executors.newFixedThreadPool(2);
    List<Future> queries = new ArrayList<>();
    for (String sourceTable : sourceDestTables.keySet()) {
      Future<?> adjustment = service.submit(() -> {
        try {
          startStream(4, sourceTable, "adjustment");
        } catch (StreamingQueryException | IOException | InterruptedException | ClassNotFoundException e) {
          throw new RuntimeException();
        }
      });
      Thread.sleep(10000);
      Future<?> adjustment1 = service.submit(() -> {
        try {
          startStream(4, sourceTable, "adjustment1");
        } catch (StreamingQueryException | IOException | InterruptedException | ClassNotFoundException e) {
          throw new RuntimeException();
        }
      });

      try {
        adjustment.get();
      } catch (Exception e){
        System.out.println("adjustemnt failed with "+ e.getClass().getCanonicalName() +
                " message "+ e.getMessage() + " cause " + e.getCause().getClass().getCanonicalName());
        e.printStackTrace();
      }
      try {
        adjustment1.get();
      } catch (Exception e){
        System.out.println("adjustemnt1 failed with "+ e.getClass().getCanonicalName() +
                " message "+ e.getMessage() + " cause " + e.getCause().getClass().getCanonicalName());
        e.printStackTrace();
      }
//      startStream(4, sourceTable, "adjustment");
//      startStream(4, sourceTable, "ad justment1");
      //queries.add(executorService.submit(new StreamQueryExecutor(snappySpark, sourceTable, activeQueries, 5)));
    }

    executorService.shutdownNow();
//    for ( StreamingQuery q: activeQueries.values()) {
//      q.awaitTermination();
//    }

  }
  private void startStream(int attemptsLeft, String sourceTable, String tableName) throws StreamingQueryException, IOException, ClassNotFoundException, InterruptedException {
    StreamingQuery query = null;
    SnappySession snappySession = createSnappySession();
    snappySession.sql("set spark.sql.autoBroadcastJoinThreshold=-1");
    System.out.println(tableName + " starting stream. attempts left:" + attemptsLeft);
    try {
      query = startStreamQuery(snappySession,sourceTable, tableName);
      activeQueries.put(sourceTable, query);
     // snappySession.sparkContext().addSparkListener(new SparkContextListener(activeQueries));
      query.awaitTermination();
    } catch (Exception e) {
      System.out.println("stream execution "+tableName+" failed with exception:");
      System.out.println(tableName+" failed with "+ e.getClass().getCanonicalName() +
              " message "+ e.getMessage() + " cause " + e.getCause().getClass().getCanonicalName());
      e.printStackTrace();
      if(query!=null && query.isActive()){
        query.stop();
      }

      if(attemptsLeft ==0 ){
        throw e;
      }

      Thread.sleep(30000L);
      startStream(attemptsLeft - 1, sourceTable, tableName);
    }
  }

  private StreamingQuery startStreamQuery(SnappySession snappySession, String sourceTable, String tableName) throws ClassNotFoundException, IOException {

    DataStreamReader reader = snappySession.readStream()
            .format(StreamConf.JDBC_STREAM())
            .option(StreamConf.SPEC(), "io.snappydata.app.SqlServerSpec")
            .option(StreamConf.SOURCE_TABLE_NAME(), sourceTable)
            .option(StreamConf.MAX_EVENTS(), "50000")
            .options(sourceOptions);

    Dataset<Row> ds = reader.load();
    return getStreamWriter(snappySession, tableName, ds, sourceTable);
  }

  private StreamingQuery getStreamWriter(SnappySession snappySession, String tableName,
                                         Dataset<Row> reader, String sourceTable) throws IOException {

    Seq<Column> keyColumns = snappySession.sessionCatalog().getKeyColumns(tableName);
    String keyColsCSV = seqAsJavaList(keyColumns).stream()
            .map(Column::name).collect(Collectors.joining(","));
    System.out.println("Key Columns are :: " + keyColsCSV);
    return reader.writeStream()
            .trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
            .format(StreamConf.SNAPPY_SINK())
            .option("sink", ProcessEvents.class.getName())
            .option("tableName", tableName)
            .queryName(tableName)
            .option("keyColumns", keyColsCSV)
            .option("handleconflict", keyColumns != null ? "true" : "false")
            .start();
  }

  private class SparkContextListener extends SparkListener {
    Map<String, StreamingQuery> activeQueries;
    SparkContextListener(Map<String, StreamingQuery> activeQueries) {
      this.activeQueries = activeQueries;
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
      activeQueries.values().forEach(q -> {
        if (q.isActive()){
          q.stop();
        }
      });
    }
  }

  private static Properties readPropertyFile(String filePath) throws Exception {
    File file = new File(filePath);
    FileInputStream fileInput = new FileInputStream(file);
    Properties properties = new Properties();
    properties.load(fileInput);
    fileInput.close();
    return properties;
  }

  private static java.util.Map<String, String> fillSourceOptions(String[] args) throws Exception {
    java.util.Map<String, String> options = new HashMap<>();

    String connPropertiesPath = args[0];
    Properties properties = readPropertyFile(connPropertiesPath);

    Enumeration enuKeys = properties.keys();
    while (enuKeys.hasMoreElements()) {
      String key = (String)enuKeys.nextElement();
      String value = properties.getProperty(key);
      options.put(key, value);
    }
    return options;
  }

  private Map<String, String> configureTables(String[] args) throws Exception {
    String sourceDestTablePath = args[1];
    Properties properties = readPropertyFile(sourceDestTablePath);
    Enumeration enuKeys = properties.keys();
    Map<String, String> sourceDestTables = new HashMap<>();
    while (enuKeys.hasMoreElements()) {
      String key = (String)enuKeys.nextElement();
      String value = properties.getProperty(key);
      sourceDestTables.put(key, value);
    }

    return sourceDestTables;
  }

}
