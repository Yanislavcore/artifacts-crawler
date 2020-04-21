package org.yanislavcore.crawler.stream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.slf4j.LoggerFactory
import org.yanislavcore.crawler.data.ScheduledUrlData
import org.yanislavcore.crawler.service.ClusterMetRepository

import scala.concurrent.ExecutionContext

class ClusterCacheChecker(private val repo: ClusterMetRepository)
  extends AsyncFunction[ScheduledUrlData, (ScheduledUrlData, Boolean)] {

  private val log = LoggerFactory.getLogger(classOf[ClusterCacheChecker])
  private implicit val ex: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

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

object ClusterCacheChecker {
  def apply(repo: ClusterMetRepository): ClusterCacheChecker = new ClusterCacheChecker(repo)

  def getProducedType: TypeInformation[(ScheduledUrlData, Boolean)] = {
    getForClass(classOf[(ScheduledUrlData, Boolean)])
  }
}


