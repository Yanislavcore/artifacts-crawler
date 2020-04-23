package org.yanislavcore.common.stream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.yanislavcore.common.data.ScheduledUrlData


class ScheduledUrlDeserializationSchema() extends KafkaDeserializationSchema[ScheduledUrlData] {

  private lazy val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  override def isEndOfStream(nextElement: ScheduledUrlData): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): ScheduledUrlData =
    objectMapper.readValue(record.value(), classOf[ScheduledUrlData])

  override def getProducedType: TypeInformation[ScheduledUrlData] =
    ScheduledUrlDeserializationSchema.getProducedType
}

object ScheduledUrlDeserializationSchema {
  def getProducedType: TypeInformation[ScheduledUrlData] = {
    getForClass(classOf[ScheduledUrlData])
  }
}


