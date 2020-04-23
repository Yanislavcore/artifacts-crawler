package org.yanislavcore.crawler.stream

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.yanislavcore.common.data.ScheduledUrlData
import org.yanislavcore.crawler.{FlinkHelpers, Utils}

/**
 * Checks if url is artifact url.
 */
class ApkMirrorArtifactsSplitter extends ProcessFunction[ScheduledUrlData, ScheduledUrlData] {

  override def processElement(value: ScheduledUrlData,
                              ctx: ProcessFunction[ScheduledUrlData, ScheduledUrlData]#Context,
                              out: Collector[ScheduledUrlData]): Unit = {
    val isArtifact = Utils.tryUrl(value.url)
      .fold({ _ => false }, { url =>
        //I didn't found any other paths
        url.getPath.equals(ApkMirrorArtifactsSplitter.ArtifactPath)
      })

    if (!isArtifact) {
      out.collect(value)
    } else {
      ctx.output(FlinkHelpers.ArtifactTag, value)
    }
  }
}

object ApkMirrorArtifactsSplitter {
  private val ArtifactPath = "/wp-content/themes/APKMirror/download.php"

  def apply(): ApkMirrorArtifactsSplitter =
    new ApkMirrorArtifactsSplitter()
}
