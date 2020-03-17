/*
* Copyright © 2019. TIBCO Software Inc.
* This file is subject to the license terms contained
* in the license file that is distributed with this file.
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