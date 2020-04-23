package org.yanislavcore.fetcher.service

import org.yanislavcore.fetcher.data.ArchiveEntryMetadata

trait UnzipService extends Serializable {
  /**
   * Unzips to specific directory and returns metadata
   *
   * @param filePath       archive to unzip
   * @param unzipDirectory where to unzip
   * @return unziped files metadata
   */
  def unzip(filePath: String, unzipDirectory: String): List[ArchiveEntryMetadata]
}
