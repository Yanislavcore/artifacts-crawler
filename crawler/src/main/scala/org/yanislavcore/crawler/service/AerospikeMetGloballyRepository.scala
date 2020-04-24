package org.yanislavcore.crawler.service

import com.aerospike.client._
import com.aerospike.client.async.NioEventLoops
import com.aerospike.client.listener.RecordListener
import com.aerospike.client.policy.ClientPolicy
import org.yanislavcore.crawler.CrawlerConfig

import scala.concurrent.{Future, Promise}


class AerospikeMetGloballyRepository(private val cfg: CrawlerConfig) extends MetGloballyRepository {

  private lazy val eventLoops = new NioEventLoops(cfg.clusterCache.threads)
  private lazy val as: AerospikeClient = {
    val p = new ClientPolicy()
    //TODO share event loops with HTTP client
    p.eventLoops = eventLoops
    p.writePolicyDefault.expiration = cfg.clusterCache.expireAfter.toSeconds.toInt
    val timeout = cfg.clusterCache.timeout.toMillis.toInt
    p.writePolicyDefault.totalTimeout = timeout
    p.readPolicyDefault.totalTimeout = timeout
    val hosts = Host.parseHosts(cfg.clusterCache.hosts, 3000)
    new AerospikeClient(p, hosts: _*)
  }
  private lazy val ns = cfg.clusterCache.ns
  private lazy val set = cfg.clusterCache.set

  /**
   * Check if URL is already met and puts it again
   */
  override def checkAndPut(url: String): Future[Boolean] = {
    val key = new Key(ns, set, url)
    val promise = Promise[Boolean]()
    as.get(eventLoops.next(), new RecordListener {
      override def onSuccess(key: Key, record: Record): Unit = {
        promise.success(record != null)
        //Calling put, we don't even need to know result. "Fire and forget"
        as.put(eventLoops.next(), null, null, key)
      }

      override def onFailure(exception: AerospikeException): Unit = {
        promise.failure(exception)
      }
    }, null, key)

    promise.future
  }
}

object AerospikeMetGloballyRepository {
  def apply(cfg: CrawlerConfig): AerospikeMetGloballyRepository = new AerospikeMetGloballyRepository(cfg)
}
