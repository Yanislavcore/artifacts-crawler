package org.yanislavcore.crawler.stream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass
import org.yanislavcore.common.data.ScheduledUrlData


class ScheduledUrlDeserializer() extends MapFunction[String, ScheduledUrlData] {

  private lazy val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  override def map(value: String): ScheduledUrlData = {
    objectMapper.readValue(value, classOf[ScheduledUrlData])
  }
}

object ScheduledUrlDeserializer {
  def apply(): ScheduledUrlDeserializer = new ScheduledUrlDeserializer()
  def getProducedType: TypeInformation[ScheduledUrlData] = {
    getForClass(classOf[ScheduledUrlData])
  }
}
