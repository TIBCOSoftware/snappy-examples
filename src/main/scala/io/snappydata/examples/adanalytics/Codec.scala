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

package io.snappydata.examples.adanalytics

import com.miguno.kafka.avro.{AvroEncoder, AvroDecoder}

import kafka.utils.VerifiableProperties

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.streaming.StreamToRowsConverter

class AdImpressionLogAvroEncoder(props: VerifiableProperties = null)
    extends AvroEncoder[AdImpressionLog](props, AdImpressionLog.getClassSchema)

class AdImpressionLogAvroDecoder(props: VerifiableProperties = null)
    extends AvroDecoder[AdImpressionLog](props, AdImpressionLog.getClassSchema)

class KafkaStreamToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[InternalRow] = {
    val log = message.asInstanceOf[AdImpressionLog]
    Seq(InternalRow.fromSeq(Seq(log.getTimestamp,
      UTF8String.fromString(log.getPublisher.toString),
      UTF8String.fromString(log.getAdvertiser.toString),
      UTF8String.fromString(log.getWebsite.toString),
      UTF8String.fromString(log.getGeo.toString),
      log.getBid,
      UTF8String.fromString(log.getCookie.toString))))
  }
}
