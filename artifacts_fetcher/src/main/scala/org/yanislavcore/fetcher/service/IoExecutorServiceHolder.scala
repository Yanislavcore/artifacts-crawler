package org.yanislavcore.fetcher.service

import java.util.concurrent.{ExecutorService, Executors}

import com.typesafe.config.ConfigFactory

object IoExecutorServiceHolder extends ExecutorServiceHolder {
  private lazy val es = {
    val threadsNumber = ConfigFactory.load().getInt("unpacker.file-io-threads")
    Executors.newFixedThreadPool(threadsNumber)
  }

  override def getExecutorService: ExecutorService = es
}
