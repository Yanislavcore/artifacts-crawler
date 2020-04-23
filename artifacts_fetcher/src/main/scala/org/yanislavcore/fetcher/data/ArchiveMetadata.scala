package org.yanislavcore.fetcher.data

case class ArchiveMetadata(entries: List[ArchiveEntryMetadata],
                           archiveLocation: String,
                           unpackedDir: String)
