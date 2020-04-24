package org.yanislavcore.common.stream

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.message.ObjectMessage

class LoggingSink[T] extends SinkFunction[T] {
  private val log = LogManager.getLogger(classOf[LoggingSink[AnyRef]])

  override def invoke(value: T, context: SinkFunction.Context[_]): Unit = {
    log.info(new ObjectMessage(value))
  }
}

object LoggingSink {
  def apply[T](): LoggingSink[T] = new LoggingSink()
}
