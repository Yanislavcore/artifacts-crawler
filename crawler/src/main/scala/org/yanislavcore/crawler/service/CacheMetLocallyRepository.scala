package org.yanislavcore.crawler.service

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.typesafe.config.ConfigFactory

/**
 * As long as we need to create only one cache per JVM, this class encapsulates cache inside lazy field.
 */
object CacheMetLocallyRepository extends MetLocallyRepository {
  //It's better to use Set instead of Map, but this cache doesn't support it.
  //That's why we are storing one string/reference for all entries, in order to reduce overhead
  private val valueHolder: String = ""
  private lazy val cache: Cache[String, String] = {
    val cfg = ConfigFactory.load().getConfig("local-cache")
    Caffeine.newBuilder()
      .maximumSize(cfg.getLong("max-size"))
      .expireAfterWrite(cfg.getDuration("expire-after"))
      .build()
  }

  /**
   * Check if URL is already met and puts it again
   */
  override def checkAndPut(url: String): Boolean = {
    val alreadyMet: String = cache.getIfPresent(url)
    cache.put(url, valueHolder)
    alreadyMet != null
  }
}
