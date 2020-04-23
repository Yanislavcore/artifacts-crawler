package org.yanislavcore.fetcher.stream

import java.net.URL

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.slf4j.LoggerFactory
import org.yanislavcore.common.data.{FetchedData, ScheduledUrlData, UrlProcessFailure}
import org.yanislavcore.fetcher.ArtifactsFetcherConfig
import org.yanislavcore.fetcher.data.ArchiveMetadata
import org.yanislavcore.fetcher.service.{ExecutorServiceHolder, FileWriterService, UnzipService}

import scala.util.Try

class AsyncDataUnpackerFunction(private val cfg: ArtifactsFetcherConfig,
                                private val writer: FileWriterService,
                                private val unzipService: UnzipService,
                                private val esHolder: ExecutorServiceHolder)
  extends AsyncFunction[(ScheduledUrlData, FetchedData), Either[(ScheduledUrlData, UrlProcessFailure), ArchiveMetadata]] {

  private val log = LoggerFactory.getLogger(classOf[AsyncDataUnpackerFunction])
  private val unpackerCfg = cfg.unpacker

  override def asyncInvoke(input: (ScheduledUrlData, FetchedData),
                           resultFuture: ResultFuture[Either[(ScheduledUrlData, UrlProcessFailure), ArchiveMetadata]]): Unit =
    esHolder.getExecutorService.execute { () =>
      resultFuture complete Iterable(tryUnpack(input._1, input._2))
    }

  private def tryUnpack(url: ScheduledUrlData,
                        fetched: FetchedData): Either[(ScheduledUrlData, UrlProcessFailure), ArchiveMetadata] =
    Try {
      val fileName = new URL(fetched.url).getPath.split("/").last
      writer.writeFile(fetched.body, unpackerCfg.targetDir, fileName)
      val fullPath = unpackerCfg.targetDir + "/" + fileName
      val unpackedDir = unpackerCfg.targetDir + "/_unpacked_" + fileName
      val metadata = unzipService.unzip(fullPath, unpackedDir)
      ArchiveMetadata(metadata, fullPath, unpackedDir)
    }
      .toEither
      .left
      .map { t =>
        log.warn("Failed to unpack archive", t)
        (url, UrlProcessFailure("Error during artifact unpacking: " + t.getMessage))
      }

  override def timeout(input: (ScheduledUrlData, FetchedData),
                       resultFuture: ResultFuture[Either[(ScheduledUrlData, UrlProcessFailure), ArchiveMetadata]]): Unit =
    resultFuture complete Iterable(Left(input._1, UrlProcessFailure("Timeout during unpacking!")))
}

object AsyncDataUnpackerFunction {
  def apply(cfg: ArtifactsFetcherConfig,
            writer: FileWriterService,
            unzipService: UnzipService, esHolder: ExecutorServiceHolder): AsyncDataUnpackerFunction =
    new AsyncDataUnpackerFunction(cfg, writer, unzipService, esHolder)

  def getProducedType: TypeInformation[Either[(ScheduledUrlData, UrlProcessFailure), ArchiveMetadata]] = {
    getForClass(classOf[Either[(ScheduledUrlData, UrlProcessFailure), ArchiveMetadata]])
  }
}
