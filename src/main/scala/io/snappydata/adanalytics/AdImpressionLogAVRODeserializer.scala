/*
* Copyright © 2019. TIBCO Software Inc.
* This file is subject to the license terms contained
* in the license file that is distributed with this file.
*/

package io.snappydata.adanalytics

import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.{SpecificData, SpecificDatumReader}

class AdImpressionLogAVRODeserializer {
  @transient private lazy val datumReader = new SpecificDatumReader[AdImpressionLog](AdImpressionLog.getClassSchema,
    AdImpressionLog.getClassSchema, new SpecificData())
  @transient private var decoder: BinaryDecoder = _
  @transient private var result: AdImpressionLog = _

  def deserialize(data: Array[Byte]): AdImpressionLog = {
    decoder = DecoderFactory.get.binaryDecoder(data, decoder)
    result = datumReader.read(result, decoder)
    result
  }
}