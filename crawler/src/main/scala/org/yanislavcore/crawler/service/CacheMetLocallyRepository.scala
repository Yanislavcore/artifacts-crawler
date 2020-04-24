package org.yanislavcore.crawler.service

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import org.yanislavcore.crawler.CrawlerConfig

/**
 * As long as we need to create only one cache per JVM, this class encapsulates cache inside lazy field.
 */
class CacheMetLocallyRepository(cfg: CrawlerConfig) extends MetLocallyRepository {
  private lazy val cache: Cache[String, String] = {
    Caffeine.newBuilder()
      .maximumSize(cfg.localCache.maxSize)
      .expireAfterWrite(cfg.localCache.expireAfter.toMillis, TimeUnit.MILLISECONDS)
      .build()
  }
  //It's better to use Set instead of Map, but this cache doesn't support it.
  //That's why we are storing one string/reference for all entries, in order to reduce overhead
  private val valueHolder: String = ""

  /**
   * Check if URL is already met and puts it again
   */
  override def checkAndPut(url: String): Boolean = {
    val alreadyMet: String = cache.getIfPresent(url)
    cache.put(url, valueHolder)
    alreadyMet != null
  }
}

object CacheMetLocallyRepository {
  def apply(cfg: CrawlerConfig): CacheMetLocallyRepository = new CacheMetLocallyRepository(cfg)
}
