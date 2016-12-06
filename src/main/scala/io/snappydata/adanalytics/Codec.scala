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

package io.snappydata.adanalytics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import com.miguno.kafka.avro.{AvroDecoder, AvroEncoder}
import kafka.utils.VerifiableProperties

class AdImpressionLogAvroEncoder(props: VerifiableProperties = null)
  extends AvroEncoder[AdImpressionLog](props, AdImpressionLog.getClassSchema)

class AdImpressionLogAvroDecoder(props: VerifiableProperties = null)
  extends AvroDecoder[AdImpressionLog](props, AdImpressionLog.getClassSchema)

/**
  * Convertes Spark RDD[AdImpressionLog] to RDD[Row]
  * to insert into table
  */
class AdImpressionLogToRowRDD extends Serializable {

  def convert(logRdd: RDD[AdImpressionLog]): RDD[Row] = {
    logRdd.map(log => {
      Row(log.getTimestamp,
        log.getPublisher.toString,
        log.getAdvertiser.toString,
        log.getWebsite.toString,
        log.getGeo.toString,
        log.getBid,
        log.getCookie.toString)
    })
  }
}
