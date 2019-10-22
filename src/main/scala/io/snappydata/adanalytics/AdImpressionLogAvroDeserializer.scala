//todo[vatsal]: add appropreate copyright header
package io.snappydata.adanalytics

import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader

object AdImpressionLogAvroDeserializer {
  lazy val datumReader = new SpecificDatumReader[AdImpressionLog](AdImpressionLog.getClassSchema)
  val decoder = new ThreadLocal[BinaryDecoder]
  val adImpressionLog = new ThreadLocal[AdImpressionLog]

  def deserialize(data: Array[Byte]): AdImpressionLog = {
      decoder.set(DecoderFactory.get.binaryDecoder(data, decoder.get()))
      adImpressionLog.set(datumReader.read(adImpressionLog.get(), decoder.get()))
      adImpressionLog.get()
  }
}