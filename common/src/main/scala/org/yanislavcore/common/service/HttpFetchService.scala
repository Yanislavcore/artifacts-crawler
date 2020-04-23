package org.yanislavcore.common.service

import java.util
import java.util.concurrent.{Executor, ThreadLocalRandom}

import com.google.common.base.Charsets
import com.google.common.io.Resources
import com.typesafe.config.ConfigFactory
import org.asynchttpclient.Dsl.{asyncHttpClient, config}
import org.asynchttpclient.{AsyncHttpClient, RequestBuilder}
import org.yanislavcore.common.data.FetchedData

import scala.concurrent.{Future, Promise}

/**
 * As long as we need to create only one client per JVM, this class encapsulates http client inside lazy field.
 */
object HttpFetchService extends FetchService {

  //noinspection UnstableApiUsage
  private lazy val uaPool: util.ArrayList[String] = {
    val lines = Resources.readLines(Resources.getResource("userAgentsPool.txt"), Charsets.UTF_8)
    //Moved to Arraylist just to be sure
    new util.ArrayList[String](lines)
  }
  private lazy val poolSize = uaPool.size()

  private def nextUserAgent(): String = {
    val nextElement = ThreadLocalRandom.current().nextInt(0, poolSize)
    uaPool.get(nextElement)
  }

  private lazy val client: AsyncHttpClient = {
    val cfg = ConfigFactory.load().getConfig("fetcher")
    val clientCfg = config()
      .setFollowRedirect(true)
      .setIoThreadsCount(cfg.getInt("threads"))
      .setRequestTimeout(cfg.getDuration("timeout").toMillis.toInt)
    asyncHttpClient(clientCfg)
  }

  override def fetch(url: String)(implicit ex: Executor): Future[FetchedData] = {
    val req = new RequestBuilder()
      .setMethod("GET")
      .setHeader("user-agent", nextUserAgent())
      .setUrl(url)
    val promise = Promise[FetchedData]()
    val resp = client.executeRequest(req)
    resp.addListener(() => {
      try {
        val result = resp.get()
        promise success FetchedData(
          result.getStatusCode,
          result.getResponseBodyAsBytes,
          result.getContentType,
          url = result.getUri.toUrl
        )
      } catch {
        case _: Throwable => promise failure _
      }
    }, ex)
    promise.future
  }
}
