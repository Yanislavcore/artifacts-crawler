package org.yanislavcore.common.stream

import java.lang

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.streaming.connectors.kafka.{KafkaContextAware, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.yanislavcore.common.data.FailedUrlData


class FetchFailureSerializationSchema(private val topic: String)
  extends KafkaSerializationSchema[FailedUrlData]
    with KafkaContextAware[FailedUrlData] {

  private val log = LoggerFactory.getLogger(classOf[FetchFailureSerializationSchema])
  private lazy val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  override def serialize(element: FailedUrlData,
                         timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val key = element.url.getBytes("utf-8")
    val value = objectMapper.writeValueAsBytes(element)
    log.warn("Sending {}", element)
    new ProducerRecord(topic, key, value)
  }

  override def getTargetTopic(element: FailedUrlData): String = topic
}

object FetchFailureSerializationSchema {
  def apply(topic: String): FetchFailureSerializationSchema = new FetchFailureSerializationSchema(topic)
}




