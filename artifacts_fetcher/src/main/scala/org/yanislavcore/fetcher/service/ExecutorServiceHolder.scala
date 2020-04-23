package org.yanislavcore.fetcher.service

import java.util.concurrent.ExecutorService

trait ExecutorServiceHolder {
  def getExecutorService: ExecutorService
}
