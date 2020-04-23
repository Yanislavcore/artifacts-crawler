package org.yanislavcore.fetcher.service

import java.util.concurrent.ExecutorService

trait ExecutorServiceHolder extends Serializable {
  def getExecutorService: ExecutorService
}
