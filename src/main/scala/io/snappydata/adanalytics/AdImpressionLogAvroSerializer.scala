//todo[vatsal]: add appropreate copyright header
package io.snappydata.adanalytics

import java.io.ByteArrayOutputStream
import java.util

import io.snappydata.adanalytics.AdImpressionLogAvroSerializer.{binaryEncoder, datumWriter}
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}

class AdImpressionLogAvroSerializer extends org.apache.kafka.common.serialization.Serializer[AdImpressionLog] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {} //no op

  override def serialize(topic: String, data: AdImpressionLog): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    binaryEncoder.set(EncoderFactory.get.binaryEncoder(byteArrayOutputStream, binaryEncoder.get))
    datumWriter.write(data, binaryEncoder.get())
    binaryEncoder.get().flush()
    byteArrayOutputStream.close()
    byteArrayOutputStream.toByteArray
  }

  override def close(): Unit = {} //no op
}

object AdImpressionLogAvroSerializer {
  val binaryEncoder = new ThreadLocal[BinaryEncoder]
  lazy val datumWriter = new GenericDatumWriter[AdImpressionLog](AdImpressionLog.SCHEMA$)
}