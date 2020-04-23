package org.yanislavcore.fetcher.service

import java.nio.file.{Files, Paths}

import net.lingala.zip4j.ZipFile
import org.slf4j.LoggerFactory
import org.yanislavcore.fetcher.data.ArchiveEntryMetadata

import scala.collection.JavaConverters._

object UnzipServiceImpl extends UnzipService {
  private val log = LoggerFactory.getLogger(UnzipServiceImpl.getClass)

  override def unzip(filePath: String,
                     unzipDirectory: String): List[ArchiveEntryMetadata] = {
    val dirPath = Paths.get(unzipDirectory)
    if (!Files.exists(dirPath) || !Files.isDirectory(dirPath)) {
      Files.createDirectories(dirPath)
    }
    log.debug("Extracting {}", filePath)
    val f = new ZipFile(filePath)
    f.extractAll(unzipDirectory)
    log.debug("Extracted {}", filePath)
    f.getFileHeaders.asScala
      .map { h =>
        ArchiveEntryMetadata(h.getFileName, h.getUncompressedSize)
      }
      .toList
  }
}
