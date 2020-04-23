package org.yanislavcore.fetcher

import scala.concurrent.duration.Duration

case class ArtifactsFetcherConfig(fetcher: FetcherConfig,
                                  artifactsTopic: String,
                                  quarantineArtifactsTopic: String,
                                  maxProducers: Int,
                                  kafkaOptions: Map[String, String],
                                  unpacker: UnpackerConfig)

case class FetcherConfig(threads: Int, timeout: Duration)

case class UnpackerConfig(targetDir: String,
                          metaDataFile: String,
                          fileIoThreads: Int,
                          timeout: Duration)