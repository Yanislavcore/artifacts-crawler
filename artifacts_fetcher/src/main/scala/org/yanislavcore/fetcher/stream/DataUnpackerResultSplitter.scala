package org.yanislavcore.fetcher.stream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.yanislavcore.common.data.{FailedUrlData, ScheduledUrlData, UrlProcessFailure}
import org.yanislavcore.common.stream.FetchSuccessSplitter.getProducedSideType
import org.yanislavcore.fetcher.data.ArchiveMetadata

class DataUnpackerResultSplitter
  extends ProcessFunction[Either[(ScheduledUrlData, UrlProcessFailure), ArchiveMetadata], ArchiveMetadata] {
  private val log = LoggerFactory.getLogger(classOf[DataUnpackerResultSplitter])
  override def processElement(value: Either[(ScheduledUrlData, UrlProcessFailure), ArchiveMetadata],
                              ctx: ProcessFunction[Either[(ScheduledUrlData, UrlProcessFailure), ArchiveMetadata], ArchiveMetadata]#Context,
                              out: Collector[ArchiveMetadata]): Unit = {

    log.debug("Got msg! {}", value)
    if (value.isRight) {
      out.collect(value.right.get)
    } else {
      val failedData = value.left.get
      ctx.output(
        DataUnpackerResultSplitter.UnpackFailedTag,
        FailedUrlData(failedData._1.url, failedData._1.ignoreExternalUrls, failedData._2.msg)
      )
    }
  }
}

object DataUnpackerResultSplitter {
  val UnpackFailedTag: OutputTag[FailedUrlData] = OutputTag[FailedUrlData]("unpackFailedStream")(getProducedSideType)
  def apply(): DataUnpackerResultSplitter = new DataUnpackerResultSplitter()

  def getProducedType: TypeInformation[ArchiveMetadata] =
    getForClass(classOf[ArchiveMetadata])
}
