package org.yanislavcore.fetcher.stream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.yanislavcore.common.data.{FailedUrlData, ScheduledUrlData, UrlProcessFailure}
import org.yanislavcore.common.stream.FetchSuccessSplitter
import org.yanislavcore.fetcher.data.ArchiveMetadata

class DataUnpackerResultSplitter
  extends ProcessFunction[Either[(ScheduledUrlData, UrlProcessFailure), ArchiveMetadata], ArchiveMetadata] {
  override def processElement(value: Either[(ScheduledUrlData, UrlProcessFailure), ArchiveMetadata],
                              ctx: ProcessFunction[Either[(ScheduledUrlData, UrlProcessFailure), ArchiveMetadata], ArchiveMetadata]#Context,
                              out: Collector[ArchiveMetadata]): Unit = {
    if (value.isRight) {
      out.collect(value.right.get)
    } else {
      val failedData = value.left.get
      ctx.output(
        FetchSuccessSplitter.FetchFailedTag,
        FailedUrlData(failedData._1.url, failedData._1.ignoreExternalUrls, failedData._2.msg)
      )
    }
  }
}

object DataUnpackerResultSplitter {
  def apply(): DataUnpackerResultSplitter = new DataUnpackerResultSplitter()

  def getProducedType: TypeInformation[ArchiveMetadata] =
    getForClass(classOf[ArchiveMetadata])
}
