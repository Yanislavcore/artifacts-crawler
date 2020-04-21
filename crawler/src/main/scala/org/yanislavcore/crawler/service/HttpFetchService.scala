package org.yanislavcore.crawler.service


import java.util.concurrent.Executor

import com.typesafe.config.ConfigFactory
import org.asynchttpclient.Dsl.{asyncHttpClient, config}
import org.asynchttpclient.{AsyncHttpClient, RequestBuilder}
import org.yanislavcore.crawler.data.FetchSuccess

import scala.concurrent.{Future, Promise}

/**
 * As long as we need to create only one client per JVM, this class encapsulates http client inside lazy field.
 */
object HttpFetchService extends FetchService {

  private lazy val client: AsyncHttpClient = {
    val cfg = ConfigFactory.load().getConfig("fetcher")
    val clientCfg = config()
      .setIoThreadsCount(cfg.getInt("threads"))
      .setRequestTimeout(cfg.getDuration("timeout").toMillis.toInt)
    asyncHttpClient(clientCfg)
  }

  override def fetch(url: String)(implicit ex: Executor): Future[FetchSuccess] = {
    val req = new RequestBuilder()
      .setMethod("GET")
      .setUrl(url)
    val promise = Promise[FetchSuccess]()
    val resp = client.executeRequest(req)
    resp.addListener(() => {
      try {
        val result = resp.get()
        promise success FetchSuccess(
          result.getStatusCode,
          result.getResponseBodyAsBytes,
          result.getContentType
        )
      } catch {
        case _: Throwable => promise failure _
      }
    }, ex)
    promise.future
  }
}
