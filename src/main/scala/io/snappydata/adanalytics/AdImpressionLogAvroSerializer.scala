package io.snappydata.adanalytics

import java.io.ByteArrayOutputStream
import java.util

import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory

class AdImpressionLogAvroSerializer extends org.apache.kafka.common.serialization.Serializer[AdImpressionLog] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = _ //no op

  override def serialize(topic: String, data: AdImpressionLog): Array[Byte] = {
    if (data != null) {
      val byteArrayOutputStream = new ByteArrayOutputStream()
      val binaryEncoder = EncoderFactory.get.binaryEncoder(byteArrayOutputStream, null)
      val datumWriter = new GenericDatumWriter[AdImpressionLog](data.getSchema)
      datumWriter.write(data, binaryEncoder)
      binaryEncoder.flush()
      byteArrayOutputStream.close()
      byteArrayOutputStream.toByteArray
    } else null
  }

  override def close(): Unit = _ //no op
}
