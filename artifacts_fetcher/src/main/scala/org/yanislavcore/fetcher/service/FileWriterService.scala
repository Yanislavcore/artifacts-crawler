package org.yanislavcore.fetcher.service

trait FileWriterService extends Serializable {
  def writeFile(array: Array[Byte], directory: String, name: String): Unit
}
