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
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SnappySession;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.ProcessingTime;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.jdbc.StreamConf;
import org.apache.spark.util.Utils;

public class JavaCdcStreamingApp {

  /**
   * A map of source and destination table. The source table should have been defined in
   * the source database and destination table in SnappyData
   */
  private final java.util.Map<String, String> sourceDestTables = new LinkedHashMap<>();
  private final java.util.Map<String, String> tableKeyMap = new LinkedHashMap<>();

  private java.util.Map<String, String> sourceOptions = new HashMap<>();

  private SnappySession snappySpark;

  public static void main(String[] args) throws Exception {
    JavaCdcStreamingApp _this = new JavaCdcStreamingApp();
    _this.initSourceOptions(args);
    _this.startJob(args);
  }

  private SnappySession createSnappySession(String table) throws ClassNotFoundException, IOException {
    String checkPointDir = Utils.createTempDir(".", "stream-spark-"+table).getCanonicalPath();
    snappySpark = new SnappySession(SparkSession.builder()
        .config("spark.sql.streaming.checkpointLocation", checkPointDir)
        .getOrCreate().sparkContext());

    return snappySpark;
  }

  private void initSourceOptions(String[] args) throws Exception {
    sourceOptions = fillSourceOptions(args);
  }

  private void startJob(String[] args) throws Exception {
    configureTables(args);
    ArrayList<StreamingQuery> activeQueries = new ArrayList<>(sourceDestTables.size());
    System.out.println("SourceOptiona are " + sourceOptions);

    for (String sourceTable : sourceDestTables.keySet()) {
      SnappySession newSession = createSnappySession(sourceTable);
      DataStreamReader reader = newSession.readStream()
          .format(StreamConf.JDBC_STREAM())
          .option(StreamConf.SPEC(), "io.snappydata.app.SqlServerSpec")
          .option(StreamConf.SOURCE_TABLE_NAME(), sourceTable)
          .option(StreamConf.MAX_EVENTS(), "50000")
          .options(sourceOptions);

      Dataset<Row> ds = reader.load();
      StreamingQuery q = getStreamWriter(sourceDestTables.get(sourceTable), ds);
      activeQueries.add(q);
    }

    snappySpark.sparkContext().addSparkListener(new SparkContextListener(activeQueries));

    for (StreamingQuery q : activeQueries) {
      q.awaitTermination();
    }
  }

  private class SparkContextListener extends SparkListener {
    ArrayList<StreamingQuery> activeQueries;
    public SparkContextListener(ArrayList<StreamingQuery> activeQueries) {
      this.activeQueries = activeQueries;
    }
    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
      activeQueries.stream().forEach(q -> {
        if (q.isActive()){
          q.stop();
        }
      });
    }
  }

  protected StreamingQuery getStreamWriter(String tableName,
      Dataset<Row> reader) throws IOException {

    String keyColumns = tableKeyMap.get(tableName);
    System.out.println("keyColumns are " + keyColumns);
    return reader.writeStream()
        .trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
        .format(StreamConf.SNAPPY_SINK())
        .option("sink", ProcessEvents.class.getName())
        .option("tableName", tableName)
        .option("keyColumns", keyColumns)
        .option("handleconflict", keyColumns != null ? "true" : "false")
        .start();
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

  private void configureTables(String[] args) throws Exception {
    String sourceDestTablePath = args[1];
    String tableKeyMapPath = args[2];
    Properties properties = readPropertyFile(sourceDestTablePath);
    Properties tableKeysProps = readPropertyFile(tableKeyMapPath);

    Enumeration enuKeys = properties.keys();
    while (enuKeys.hasMoreElements()) {
      String key = (String)enuKeys.nextElement();
      String value = properties.getProperty(key);
      sourceDestTables.put(key, value);
    }

    Enumeration tableKeysEnum = tableKeysProps.keys();
    while (tableKeysEnum.hasMoreElements()) {
      String key = (String)tableKeysEnum.nextElement();
      String value = tableKeysProps.getProperty(key);
      tableKeyMap.put(key, value);
    }
  }

}
