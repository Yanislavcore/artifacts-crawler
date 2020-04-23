package org.yanislavcore.fetcher.service

import java.nio.file.{Files, Paths}

object FileWriterServiceImpl extends FileWriterService {
  override def writeFile(array: Array[Byte], directory: String, name: String): Unit = {
    val dirPath = Paths.get(directory)
    if (!Files.exists(dirPath) || !Files.isDirectory(dirPath)) {
      Files.createDirectories(dirPath)
    }
    val filePath = Paths.get(directory, name)
    Files.write(filePath, array)
  }
}
