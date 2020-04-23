package org.yanislavcore.crawler.stream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.slf4j.LoggerFactory
import org.yanislavcore.common.data.ScheduledUrlData
import org.yanislavcore.crawler.service.MetGloballyRepository

import scala.concurrent.ExecutionContext

class MetGloballyChecker(private val repo: MetGloballyRepository)
  extends AsyncFunction[ScheduledUrlData, (ScheduledUrlData, Boolean)] {

  private val log = LoggerFactory.getLogger(classOf[MetGloballyChecker])
  private lazy implicit val ex: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

  override def asyncInvoke(input: ScheduledUrlData, resultFuture: ResultFuture[(ScheduledUrlData, Boolean)]): Unit = {
    repo.checkAndPut(input.url).onComplete { result =>

      val status: Boolean = result.fold({ e =>
        log.warn("Error, while doing request to AS", e)
        false
      }, identity)
      resultFuture.complete(Iterable((input, status)))
    }
  }

  override def timeout(input: ScheduledUrlData, resultFuture: ResultFuture[(ScheduledUrlData, Boolean)]): Unit = {
    log.warn("Timeout, while obtaining met data from AS")
    resultFuture.complete(Iterable((input, false)))
  }
}

object MetGloballyChecker {
  def apply(repo: MetGloballyRepository): MetGloballyChecker = new MetGloballyChecker(repo)

  def getProducedType: TypeInformation[(ScheduledUrlData, Boolean)] = {
    getForClass(classOf[(ScheduledUrlData, Boolean)])
  }
}


