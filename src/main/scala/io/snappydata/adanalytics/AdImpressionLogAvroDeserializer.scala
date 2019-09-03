package io.snappydata.adanalytics

import java.util

import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.kafka.common.serialization.Deserializer

class AdImpressionLogAvroDeserializer extends Deserializer[AdImpressionLog] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = _ //no op
  override def close(): Unit = _ //no op
  override def deserialize(topic: String, data: Array[Byte]): AdImpressionLog = {
    if (data != null) {
      val datumReader = new SpecificDatumReader[AdImpressionLog](AdImpressionLog.getClassSchema)
      val decoder = DecoderFactory.get.binaryDecoder(data, null)
      datumReader.read(null , decoder)
    } else null
  }
}
