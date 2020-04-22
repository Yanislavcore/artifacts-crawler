package org.yanislavcore.crawler

import scala.concurrent.duration.Duration

case class CrawlerConfig(kafkaOptions: Map[String, String],
                         urlsTopic: String,
                         maxProducers: Int,
                         artifactsTopic: String,
                         fetcher: FetcherConfig,
                         ignoredExtensions: List[String],
                         localCache: LocalCacheConfig,
                         clusterCache: ClusterCacheConfig)

case class FetcherConfig(threads: Int, timeout: Duration)

case class LocalCacheConfig(maxSize: Long, expireAfter: Duration)

case class ClusterCacheConfig(
                               expireAfter: Duration,
                               timeout: Duration,
                               threads: Int,
                               ns: String,
                               set: String,
                               hosts: String
                             )