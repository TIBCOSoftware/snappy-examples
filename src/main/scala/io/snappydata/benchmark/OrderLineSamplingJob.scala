package io.snappydata.benchmark

import com.typesafe.config.Config
import org.apache.spark.sql.SnappySQLJob
import spark.jobserver.{SparkJobValid, SparkJobValidation}

class OrderLineSamplingJob extends SnappySQLJob {

  override def runJob(sc: C, jobConfig: Config): Any = {

    sc.sql("drop table if exists oorder_col")
    sc.sql("drop table if exists sampled_order_line_col")
    sc.sql("drop table if exists order_line_col")

    sc.sql("create table oorder_col (" +
      "o_w_id       integer," +
      "o_d_id       integer," +
      "o_id         integer," +
      "o_c_id       integer," +
      "o_carrier_id integer," +
      "o_ol_cnt     decimal(2,0)," +
      "o_all_local  decimal(1,0)," +
      "o_entry_d    timestamp " +
      ") using column options( partition_by 'o_w_id, o_d_id, o_id', buckets '41')")

//    val ordersDF = sc.read
//      .format("com.databricks.spark.csv")
//      .option("inferSchema", "true")
//      .load("/home/ymahajan/6W/ORDERS.csv")
//
//    ordersDF.show(10)
//
//    ordersDF.write.insertInto("oorder_col")

    sc.sql("create table order_line_col(" +
      "ol_w_id         integer," +
      "ol_d_id         integer," +
      "ol_o_id         integer," +
      "ol_number       integer," +
      "ol_i_id         integer," +
      "ol_delivery_d   timestamp," +
      "ol_amount       decimal(6,2)," +
      "ol_supply_w_id  integer," +
      "ol_quantity     decimal(2,0)," +
      "ol_dist_info    varchar(24))" +
      " using column options( partition_by 'ol_w_id, ol_d_id, ol_o_id'," +
      " colocate_with 'oorder_col', buckets '41')")

    sc.sql("CREATE SAMPLE TABLE sampled_order_line_col" +
      " OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50', baseTable 'order_line_col')")

    val orderLineDF = sc.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .load("/home/ymahajan/6W/ORDER_LINE.csv")

    orderLineDF.write.insertInto("order_line_col")

    orderLineDF.write.insertInto("sampled_order_line_col")
  }

  override def validate(sc: C, config: Config): SparkJobValidation = {
    SparkJobValid
  }
}
