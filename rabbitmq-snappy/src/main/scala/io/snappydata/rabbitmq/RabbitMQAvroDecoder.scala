package io.snappydata.rabbitmq

import io.snappydata.adanalytics.AdImpressionLog
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.sql.streaming.RabbitMQDecoder

class RabbitMQAvroDecoder extends RabbitMQDecoder[AdImpressionLog] {
  def fromBytes(bytes: scala.Array[scala.Byte]): AdImpressionLog = {
    val reader = new SpecificDatumReader[AdImpressionLog](AdImpressionLog.getClassSchema())
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    reader.read(null, decoder)
  }
}