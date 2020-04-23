package org.yanislavcore.fetcher.service

import net.lingala.zip4j.ZipFile
import org.yanislavcore.fetcher.data.ArchiveEntryMetadata

import scala.collection.JavaConverters._

object UnzipServiceImpl extends UnzipService {

  override def unzip(filePath: String,
                     unzipDirectory: String): List[ArchiveEntryMetadata] = {
    val f = new ZipFile(filePath)
    f.extractAll(unzipDirectory)
    f.getFileHeaders.asScala
      .map { h =>
        ArchiveEntryMetadata(h.getFileName, h.getUncompressedSize)
      }
      .toList
  }
}
