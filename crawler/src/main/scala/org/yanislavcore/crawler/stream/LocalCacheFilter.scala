package org.yanislavcore.crawler.stream

import org.apache.flink.api.common.functions.FilterFunction
import org.yanislavcore.crawler.data.ScheduledUrlData
import org.yanislavcore.crawler.service.LocalMetRepository

class LocalCacheFilter(private val metRepo: LocalMetRepository) extends FilterFunction[ScheduledUrlData]{
  override def filter(value: ScheduledUrlData): Boolean = !metRepo.checkAndPut(value.url)
}

object LocalCacheFilter {
  def apply(repo: LocalMetRepository): LocalCacheFilter = new LocalCacheFilter(repo)
}
