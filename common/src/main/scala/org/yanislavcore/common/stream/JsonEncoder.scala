package org.yanislavcore.common.stream

import java.io.OutputStream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.serialization.Encoder

class JsonEncoder[T] extends Encoder[T] {
  private lazy val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private lazy val lineSeparator: Byte = '\n'.toByte

  override def encode(element: T, stream: OutputStream): Unit = {
    stream.write(objectMapper.writeValueAsBytes(element))
    stream.write(lineSeparator)
  }
}

object JsonEncoder {
  def apply[T](): JsonEncoder[T] = new JsonEncoder()
}
