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
