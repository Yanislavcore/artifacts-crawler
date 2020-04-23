package org.yanislavcore.crawler.stream

import org.apache.flink.api.common.functions.FilterFunction
import org.yanislavcore.common.data.ScheduledUrlData
import org.yanislavcore.crawler.service.MetLocallyRepository

/**
 * Checks if url is already met in local cache.
 * Simple but powerful optimization.
 * @param metRepo - cache repo
 */
class LocalCacheFilter(private val metRepo: MetLocallyRepository) extends FilterFunction[ScheduledUrlData]{
  override def filter(value: ScheduledUrlData): Boolean = !metRepo.checkAndPut(value.url)
}

object LocalCacheFilter {
  def apply(repo: MetLocallyRepository): LocalCacheFilter = new LocalCacheFilter(repo)
}
