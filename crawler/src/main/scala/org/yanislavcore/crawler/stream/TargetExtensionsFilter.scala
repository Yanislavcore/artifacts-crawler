package org.yanislavcore.crawler.stream

import org.apache.flink.api.common.functions.FilterFunction
import org.yanislavcore.crawler.{CrawlerConfig, Utils}
import org.yanislavcore.crawler.data.ScheduledUrlData

/**
 * Filters urls by extension.
 * @param shouldSkipMatched if true will pass only non-matched, else will pass only matched
 * @param cfg crawler config
 */
class TargetExtensionsFilter(private val shouldSkipMatched: Boolean, cfg: CrawlerConfig)
  extends FilterFunction[ScheduledUrlData]{
  private val targetExtensions = cfg.targetExtensions.toSet

  override def filter(value: ScheduledUrlData): Boolean = {
    Utils.tryUrl(value.url).fold({_ => false}, { url =>
      val extension = url.getPath.split(".").last
      if (shouldSkipMatched) {
        !targetExtensions.contains(extension)
      } else {
        targetExtensions.contains(extension)
      }
    })
  }
}

object TargetExtensionsFilter {
  def apply(shouldSkipMatched: Boolean, cfg: CrawlerConfig): TargetExtensionsFilter =
    new TargetExtensionsFilter(shouldSkipMatched, cfg)
}
