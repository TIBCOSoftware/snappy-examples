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

import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SnappySession;
import org.apache.spark.sql.streaming.jdbc.SnappyStreamSink;

import static java.util.Arrays.asList;
import static org.apache.spark.SnappyJavaUtils.snappyJavaUtil;

public class ProcessEvents implements SnappyStreamSink {

  private static Logger log = Logger.getLogger(ProcessEvents.class.getName());

  private static List<String> metaColumns = asList("__$start_lsn",
      "__$end_lsn", "__$seqval", "__$operation", "__$update_mask", "__$command_id");

  @Override
  public void process(SnappySession snappySession, Properties sinkProps,
      long batchId, Dataset<Row> df) {

    String snappyTable = sinkProps.getProperty("tablename").toUpperCase();

    log.info("SB: Processing for " + snappyTable + " batchId " + batchId);

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
}
