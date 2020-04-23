package org.yanislavcore.common.stream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.yanislavcore.common.data.{FailedUrlData, FetchFailureData, FetchSuccessData, ScheduledUrlData}

class FetchSuccessSplitter
  extends ProcessFunction[(ScheduledUrlData, Either[FetchFailureData, FetchSuccessData]), (ScheduledUrlData, FetchSuccessData)] {

  override def processElement(value: (ScheduledUrlData, Either[FetchFailureData, FetchSuccessData]),
                              ctx: ProcessFunction[(ScheduledUrlData, Either[FetchFailureData, FetchSuccessData]), (ScheduledUrlData, FetchSuccessData)]#Context,
                              out: Collector[(ScheduledUrlData, FetchSuccessData)]): Unit = {
    val either = value._2
    if (either.isRight && either.getOrElse(null).code / 100 == 2) {
      out.collect((value._1, value._2.right.get))
    } else {
      val failure: FetchFailureData = either.fold(
        identity,
        { success => FetchFailureData("Response code: " + success.code) }
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
