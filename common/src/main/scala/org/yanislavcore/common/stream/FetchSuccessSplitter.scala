package org.yanislavcore.common.stream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.yanislavcore.common.data.{FailedUrlData, FetchedData, ScheduledUrlData, UrlProcessFailure}

class FetchSuccessSplitter
  extends ProcessFunction[(ScheduledUrlData, Either[UrlProcessFailure, FetchedData]), (ScheduledUrlData, FetchedData)] {
  private val log = LoggerFactory.getLogger(classOf[FetchSuccessSplitter])

  override def processElement(value: (ScheduledUrlData, Either[UrlProcessFailure, FetchedData]),
                              ctx: ProcessFunction[(ScheduledUrlData, Either[UrlProcessFailure, FetchedData]), (ScheduledUrlData, FetchedData)]#Context,
                              out: Collector[(ScheduledUrlData, FetchedData)]): Unit = {
    val either = value._2
    if (either.isRight && either.getOrElse(null).code / 100 == 2) {
      out.collect((value._1, value._2.right.get))
    } else {
      log.warn("Failed to fetch! Data: {}", value)
      val failure: UrlProcessFailure = either.fold(
        identity,
        { success => UrlProcessFailure("Response code: " + success.code) }
      )
      ctx.output(
        FetchSuccessSplitter.FetchFailedTag,
        FailedUrlData(value._1.url, value._1.ignoreExternalUrls, failure.msg)
      )
    }
  }
}

object FetchSuccessSplitter {
  val FetchFailedTag: OutputTag[FailedUrlData] =
    OutputTag[FailedUrlData]("fetchedFailedStream")(getProducedSideType)

  def apply(): FetchSuccessSplitter = new FetchSuccessSplitter()

  def getProducedSideType: TypeInformation[FailedUrlData] = {
    getForClass(classOf[FailedUrlData])
  }
}
