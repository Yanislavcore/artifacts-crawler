package org.yanislavcore.crawler.stream

import java.lang

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.streaming.connectors.kafka.{KafkaContextAware, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord
import org.yanislavcore.common.data.ScheduledUrlData


class ScheduledUrlSerializationSchema(private val topic: String) extends KafkaSerializationSchema[ScheduledUrlData]
  with KafkaContextAware[ScheduledUrlData] {

  private lazy val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  override def serialize(element: ScheduledUrlData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val key = element.url.getBytes("utf-8")
    new ProducerRecord(topic, key, objectMapper.writeValueAsBytes(element))
  }

  override def getTargetTopic(element: ScheduledUrlData): String = topic
}


object ScheduledUrlSerializationSchema {
  def apply(topic: String): ScheduledUrlSerializationSchema = new ScheduledUrlSerializationSchema(topic)
}

