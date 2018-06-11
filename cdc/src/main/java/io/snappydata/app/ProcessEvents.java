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

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SnappySession;
import org.apache.spark.sql.streaming.jdbc.SnappyStreamSink;

import static java.util.Arrays.asList;
import static org.apache.spark.SnappyJavaUtils.snappyJavaUtil;
import static org.apache.spark.sql.functions.col;

public class ProcessEvents implements SnappyStreamSink {

  private static final Logger log = Logger.getLogger(ProcessEvents.class.getName());

  private static final List<String> metaColumns = asList("__$start_lsn",
      "__$end_lsn", "__$seqval", "__$operation", "__$update_mask", "__$command_id", "STRLSN");

  private static final String[] metaColumnsArray =
      metaColumns.toArray(new String[metaColumns.size()]);

  @Override
  public void process(SnappySession snappySession, Properties sinkProps,
      long batchId, Dataset<Row> df) {

    String snappyTable = sinkProps.getProperty("tablename").toUpperCase();
    boolean handleConflict = Boolean.parseBoolean(sinkProps.getProperty("handleconflict"));

    // If for some table we are sure not to handle conflicting property keep it simple
    if (!handleConflict) {
      simpleProcess(snappySession, sinkProps, batchId, df);
      return;
    }

    log.debug("Processing for " + snappyTable + " batchId " + batchId + " With conflicting keys");

    // String separated key columns. This should match with column
    // table key columns or row table primary key.
    String commaSepratedKeyColumns = (String)sinkProps.get("keycolumns");

    List<String> keyColumns = Arrays.asList(commaSepratedKeyColumns.split(","));

      /* --------------[ Preferred Way ] ---------------- */
    df.cache();

    /**
     * Basic algorithm for recording all events are
     * a) Filter out all updates on keys which are followed by a delete.
     * b) If we get something in #a first apply those updates.
     * c) Then apply all deletes.
     * d) If count of #a is greater than zero then filter out such updates from main update set.
     *    Then apply the update.
     */
    Dataset<Row> snappyCustomerUpsert = df
        // pick only insert/update ops
        .filter("\"__$operation\" = 4 OR \"__$operation\" = 2");


    Dataset<Row> snappyCustomerDelete = df
        // pick only delete ops
        .filter("\"__$operation\" = 1");


    if (snappyCustomerDelete.count() > 0) {
      // Filter out all inserts which are before a delete by comparing their LSN numbers.
      // We are checking less than or equal to as one transaction might do both the operations.
      Column joinExpr = joinExpresssion(keyColumns, "u", "d");
      Dataset<Row> insertsFollowedByDeletes = snappyCustomerUpsert.as("u").join(snappyCustomerDelete.as("d"),
          joinExpr.and(col("u.STRLSN").leq(col("d.STRLSN"))), "left_semi");

      long insertFollowedByDeleteCount = insertsFollowedByDeletes.count();

      if (insertFollowedByDeleteCount > 0L) {
        insertsFollowedByDeletes.show();
        Dataset<Row> modifiedUpdate = insertsFollowedByDeletes
            .drop(metaColumnsArray);

        snappyJavaUtil(modifiedUpdate.write()).putInto("APP." + snappyTable);
      }

      Dataset<Row> modifiedDelete = snappyCustomerDelete
          .drop(metaColumnsArray);
      snappyJavaUtil(modifiedDelete.write()).deleteFrom("APP." + snappyTable);

      if (insertFollowedByDeleteCount > 0L) {
        insertsFollowedByDeletes.cache();
        // Filter out such updates from the main update set.
        Column joinExpr2 = joinExpresssion(keyColumns, "up", "ud");
        Dataset<Row> filteredUpdates = snappyCustomerUpsert.as("up").join(insertsFollowedByDeletes.as("ud"),
            joinExpr2.and(col("up.STRLSN").equalTo(col("ud.STRLSN"))), "left_anti");


        Dataset<Row> afterDrop = filteredUpdates
            .drop(metaColumnsArray);
        snappyJavaUtil(afterDrop.write()).putInto("APP." + snappyTable);
      } else {
        Dataset<Row> modifiedUpdate = snappyCustomerUpsert
            .drop(metaColumnsArray);
        snappyJavaUtil(modifiedUpdate.write()).putInto("APP." + snappyTable);
      }
    } else {
      Dataset<Row> modifiedUpdate = snappyCustomerUpsert
          .drop(metaColumnsArray);
      snappyJavaUtil(modifiedUpdate.write()).putInto("APP." + snappyTable);
    }
  }

  public void simpleProcess(SnappySession snappySession, Properties sinkProps,
      long batchId, Dataset<Row> df) {

    String snappyTable = sinkProps.getProperty("tablename").toUpperCase();

    log.debug("SB: Processing for " + snappyTable + " batchId " + batchId);

      /* --------------[ Preferred Way ] ---------------- */
    df.cache();

    Dataset<Row> snappyCustomerDelete = df
        // pick only delete ops
        .filter("\"__$operation\" = 1")
        // exclude the first 5 columns and pick the columns that needs to control
        // the WHERE clause of the delete operation.
        .drop(metaColumns.toArray(new String[metaColumns.size()]));

    if(snappyCustomerDelete.count() > 0) {
      snappyJavaUtil(snappyCustomerDelete.write()).deleteFrom("APP." + snappyTable);
    }

    Dataset<Row> snappyCustomerUpsert = df
        // pick only insert/update ops
        .filter("\"__$operation\" = 4 OR \"__$operation\" = 2")
        .drop(metaColumns.toArray(new String[metaColumns.size()]));
    snappyJavaUtil(snappyCustomerUpsert.write()).putInto("APP." + snappyTable);

  }

  private Column joinExpresssion(List<String> columnKeys, String alias1, String alias2) {
    List<Column> equalsStream = columnKeys.stream().map(s ->
        col(alias1 + "." + s).equalTo(col(alias2 + "." + s)))
        .collect(Collectors.toList());
    return equalsStream.stream().reduce((c1, c2) -> c1.and(c1)).get();
  }
}
