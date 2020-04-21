package org.yanislavcore.crawler.service
import com.aerospike.client.{AerospikeClient, AerospikeException, Key, Record}
import com.aerospike.client.async.NettyEventLoops
import com.aerospike.client.listener.RecordListener
import com.aerospike.client.policy.{ClientPolicy, WritePolicy}
import com.typesafe.config.ConfigFactory
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup

import scala.concurrent.{Future, Promise}

object AerospikeClusterMetRepository extends ClusterMetRepository {

  private lazy val cfg = ConfigFactory.load().getConfig("cluster-cache")
  private lazy val as: AerospikeClient = {

    val p = new ClientPolicy()
    //TODO share event loops with HTTP client
    if (System.getProperty("os.name").equalsIgnoreCase("Linux")) {
      p.eventLoops = new NettyEventLoops(new EpollEventLoopGroup(cfg.getInt("threads")))
    } else {
      p.eventLoops = new NettyEventLoops(new NioEventLoopGroup(cfg.getInt("threads")))
    }
    p.writePolicyDefault.expiration = cfg.getDuration("expire-after").toSeconds.toInt
    val timeout = cfg.getDuration("timeout").toMillis.toInt
    p.writePolicyDefault.totalTimeout = timeout
    p.readPolicyDefault.totalTimeout = timeout
    new AerospikeClient(p)
  }
  private lazy val ns = cfg.getString("ns")
  private lazy val set = cfg.getString("set")
  /**
   * Check if URL is already met and puts it again
   */
  override def checkAndPut(url: String): Future[Boolean] = {
    val key = new Key(ns, set, url)
    val promise = Promise[Boolean]()
    as.get(null, new RecordListener {
      override def onSuccess(key: Key, record: Record): Unit = {
        promise.success(record != null)
        //Calling put, we don't even need to know result. "Fire and forget"
        as.put(null, null, null, key)
      }

      override def onFailure(exception: AerospikeException): Unit = {
        promise.failure(exception)
      }
    }, null, key)

    promise.future
  }
}
