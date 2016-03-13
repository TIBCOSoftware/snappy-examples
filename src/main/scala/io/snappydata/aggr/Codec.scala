package io.snappydata.aggr

import com.miguno.kafka.avro.{AvroEncoder, AvroDecoder}

import kafka.utils.VerifiableProperties

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.streaming.StreamToRowsConverter

class ImpressionLogAvroEncoder(props: VerifiableProperties = null)
    extends AvroEncoder[ImpressionLog](props, ImpressionLog.getClassSchema)

class ImpressionLogAvroDecoder(props: VerifiableProperties = null)
    extends AvroDecoder[ImpressionLog](props, ImpressionLog.getClassSchema)

class KafkaStreamToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[InternalRow] = {
    val log = message.asInstanceOf[ImpressionLog]
    Seq(InternalRow.fromSeq(Seq(log.getTimestamp,
      UTF8String.fromString(log.getPublisher.toString),
      UTF8String.fromString(log.getAdvertiser.toString),
      UTF8String.fromString(log.getWebsite.toString),
      UTF8String.fromString(log.getGeo.toString),
      log.getBid,
      UTF8String.fromString(log.getCookie.toString))))
  }
}
