package org.yanislavcore.common.stream

import java.util.concurrent.Executor

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.yanislavcore.common.data.{FetchFailureData, FetchSuccessData, ScheduledUrlData}
import org.yanislavcore.common.service.FetchService

import scala.concurrent.ExecutionContext


class AsyncUrlFetchFunction(private val fetchService: FetchService)
  extends AsyncFunction[ScheduledUrlData, (ScheduledUrlData, Either[FetchFailureData, FetchSuccessData])] {

  implicit lazy val executor: Executor = Executors.directExecutor()
  implicit lazy val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

  override def asyncInvoke(input: ScheduledUrlData,
                           resultFuture: ResultFuture[(ScheduledUrlData, Either[FetchFailureData, FetchSuccessData])]): Unit = {
    fetchService.fetch(input.url).onComplete { resp =>
      val mapped = resp.toEither
        .left
        .map(t => FetchFailureData(t.getMessage))
      resultFuture.complete(Iterable((input, mapped)))
    }
  }

  override def timeout(input: ScheduledUrlData,
                       resultFuture: ResultFuture[(ScheduledUrlData, Either[FetchFailureData, FetchSuccessData])]): Unit = {
    resultFuture.complete(Iterable((input, Left(FetchFailureData("Timeout, during url processing")))))
  }
}

object AsyncUrlFetchFunction {
  def apply(fetchService: FetchService): AsyncUrlFetchFunction = new AsyncUrlFetchFunction(fetchService)

  def getProducedType: TypeInformation[(ScheduledUrlData, Either[FetchFailureData, FetchSuccessData])] = {
    getForClass(classOf[(ScheduledUrlData, Either[FetchFailureData, FetchSuccessData])])
  }
}