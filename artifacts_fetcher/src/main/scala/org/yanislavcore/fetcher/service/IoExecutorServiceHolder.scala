package org.yanislavcore.fetcher.service

import java.util.concurrent.{ExecutorService, Executors}

import org.yanislavcore.fetcher.ArtifactsFetcherConfig

class IoExecutorServiceHolder(cfg: ArtifactsFetcherConfig) extends ExecutorServiceHolder {
  private lazy val es = {
    Executors.newFixedThreadPool(cfg.unpacker.fileIoThreads)
  }

  override def getExecutorService: ExecutorService = es
}

object IoExecutorServiceHolder {
  def apply(cfg: ArtifactsFetcherConfig): IoExecutorServiceHolder = new IoExecutorServiceHolder(cfg)
}
