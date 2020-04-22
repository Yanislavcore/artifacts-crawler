package org.yanislavcore.crawler.stream

import java.lang

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.streaming.connectors.kafka.{KafkaContextAware, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord
import org.yanislavcore.crawler.data.{FailedUrlData, ScheduledUrlData}


class FetchFailedUrlsSerializationSchema(private val topic: String)
  extends KafkaSerializationSchema[FailedUrlData]
    with KafkaContextAware[ScheduledUrlData] {

  private lazy val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  override def serialize(element: FailedUrlData,
                         timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val key = element.url.getBytes("utf-8")
    val value = objectMapper.writeValueAsBytes(element)
    new ProducerRecord(topic, key, value)
  }

  override def getTargetTopic(element: ScheduledUrlData): String = topic
}

object FetchFailedUrlsSerializationSchema {
  def apply(topic: String): FetchFailedUrlsSerializationSchema = new FetchFailedUrlsSerializationSchema(topic)
}




