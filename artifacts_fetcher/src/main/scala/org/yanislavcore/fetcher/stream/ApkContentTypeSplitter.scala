package org.yanislavcore.fetcher.stream

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.yanislavcore.common.data.{FailedUrlData, FetchedData, ScheduledUrlData}
import org.yanislavcore.common.stream.FetchSuccessSplitter

/**
 * Adds additional logic to FetchSuccessSplitter - filters by ContentType as well
 */
class ApkContentTypeSplitter
  extends ProcessFunction[(ScheduledUrlData, FetchedData), (ScheduledUrlData, FetchedData)] {

  override def processElement(value: (ScheduledUrlData, FetchedData),
                              ctx: ProcessFunction[(ScheduledUrlData, FetchedData), (ScheduledUrlData, FetchedData)]#Context,
                              out: Collector[(ScheduledUrlData, FetchedData)]): Unit = {
    if (value._2.contentType.equalsIgnoreCase(ApkContentTypeSplitter.ApkMimeType)) {
      out.collect(value)
    } else {
      ctx.output(
        FetchSuccessSplitter.FetchFailedTag,
        FailedUrlData(value._1.url, value._1.ignoreExternalUrls, "Wrong content type: " + value._2.contentType)
      )
    }
  }
}

object ApkContentTypeSplitter {
  val ApkMimeType = "application/vnd.android.package-archive"

  def apply(): ApkContentTypeSplitter = new ApkContentTypeSplitter()
}
