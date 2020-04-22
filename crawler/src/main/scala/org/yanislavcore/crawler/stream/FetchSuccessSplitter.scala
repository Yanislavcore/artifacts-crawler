package org.yanislavcore.crawler.stream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.yanislavcore.crawler.FlinkHelpers
import org.yanislavcore.crawler.data.{FailedUrlData, FetchFailure, FetchSuccess, ScheduledUrlData}

class FetchSuccessSplitter
  extends ProcessFunction[(ScheduledUrlData, Either[FetchFailure, FetchSuccess]), (ScheduledUrlData, FetchSuccess)] {

  private val log = LoggerFactory.getLogger(classOf[FetchSuccessSplitter])
  override def processElement(value: (ScheduledUrlData, Either[FetchFailure, FetchSuccess]),
                              ctx: ProcessFunction[(ScheduledUrlData, Either[FetchFailure, FetchSuccess]), (ScheduledUrlData, FetchSuccess)]#Context,
                              out: Collector[(ScheduledUrlData, FetchSuccess)]): Unit = {
    val either = value._2
    if (either.isRight && either.getOrElse(null).code / 100 == 2) {
      out.collect((value._1, value._2.right.get))
    } else {
      val failure: FetchFailure = either.fold(
        identity,
        { success => FetchFailure("Response code: " + success.code) }
      )
      ctx.output(FlinkHelpers.FetchFailedTag,
        FailedUrlData(value._1.url, value._1.ignoreExternalUrls, failure.msg))
    }
  }
}

object FetchSuccessSplitter {
  def apply(): FetchSuccessSplitter = new FetchSuccessSplitter()

  def getProducedSideType: TypeInformation[FailedUrlData] = {
    getForClass(classOf[FailedUrlData])
  }
}
