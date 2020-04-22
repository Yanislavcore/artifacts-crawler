package org.yanislavcore.crawler.service

import com.aerospike.client._
import com.aerospike.client.async.{NettyEventLoops, NioEventLoops}
import com.aerospike.client.listener.RecordListener
import com.aerospike.client.policy.ClientPolicy
import com.typesafe.config.ConfigFactory
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup

import scala.concurrent.{Future, Promise}


object AerospikeMetGloballyRepository extends MetGloballyRepository {

  private lazy val cfg = ConfigFactory.load().getConfig("cluster-cache")
  private lazy val eventLoops = new NioEventLoops(cfg.getInt("threads"))
  private lazy val as: AerospikeClient = {

    val p = new ClientPolicy()
    //TODO share event loops with HTTP client
    p.eventLoops = eventLoops
    p.writePolicyDefault.expiration = cfg.getDuration("expire-after").toSeconds.toInt
    val timeout = cfg.getDuration("timeout").toMillis.toInt
    p.writePolicyDefault.totalTimeout = timeout
    p.readPolicyDefault.totalTimeout = timeout
    val hosts = Host.parseHosts(cfg.getString("hosts"), 3000)
    new AerospikeClient(p, hosts: _*)
  }
  private lazy val ns = cfg.getString("ns")
  private lazy val set = cfg.getString("set")

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
