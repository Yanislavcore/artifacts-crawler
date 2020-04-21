package org.yanislavcore.crawler.stream

import java.util.concurrent.Executor

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.yanislavcore.crawler.data.{FetchFailure, FetchSuccess, ScheduledUrlData}
import org.yanislavcore.crawler.service.FetchService

import scala.concurrent.ExecutionContext


class UrlFetchMapper(private val fetchService: FetchService)
  extends AsyncFunction[ScheduledUrlData, (ScheduledUrlData, Either[FetchFailure, FetchSuccess])] {

  implicit lazy val executor: Executor = Executors.directExecutor()
  implicit lazy val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

  override def asyncInvoke(input: ScheduledUrlData,
                           resultFuture: ResultFuture[(ScheduledUrlData, Either[FetchFailure, FetchSuccess])]): Unit = {
    fetchService.fetch(input.url).onComplete { resp =>
      val mapped = resp.toEither
        .left
        .map(t => FetchFailure(t.getMessage))
      resultFuture.complete(Iterable((input, mapped)))
    }
  }

  override def timeout(input: ScheduledUrlData,
                       resultFuture: ResultFuture[(ScheduledUrlData, Either[FetchFailure, FetchSuccess])]): Unit = {
    resultFuture.complete(Iterable((input, Left(FetchFailure("Timeout, during url processing")))))
  }
}

object UrlFetchMapper {
  def apply(fetchService: FetchService): UrlFetchMapper = new UrlFetchMapper(fetchService)

  def getProducedType: TypeInformation[(ScheduledUrlData, Either[FetchFailure, FetchSuccess])] = {
    getForClass(classOf[(ScheduledUrlData, Either[FetchFailure, FetchSuccess])])
  }
}