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

import java.io.ByteArrayOutputStream
import java.util

import io.snappydata.adanalytics.AdImpressionLogAVROSerializer.{binaryEncoder, datumWriter}
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}

class AdImpressionLogAVROSerializer extends org.apache.kafka.common.serialization.Serializer[AdImpressionLog] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: AdImpressionLog): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    binaryEncoder.set(EncoderFactory.get.binaryEncoder(byteArrayOutputStream, binaryEncoder.get))
    datumWriter.write(data, binaryEncoder.get())
    binaryEncoder.get().flush()
    byteArrayOutputStream.close()
    byteArrayOutputStream.toByteArray
  }

  override def close(): Unit = {}
}

object AdImpressionLogAVROSerializer {
  val binaryEncoder = new ThreadLocal[BinaryEncoder]
  lazy val datumWriter = new GenericDatumWriter[AdImpressionLog](AdImpressionLog.getClassSchema)
}