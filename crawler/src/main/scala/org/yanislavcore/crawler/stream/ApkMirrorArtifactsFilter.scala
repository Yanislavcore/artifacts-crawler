package org.yanislavcore.crawler.stream

import org.apache.flink.api.common.functions.FilterFunction
import org.yanislavcore.crawler.Utils
import org.yanislavcore.crawler.data.ScheduledUrlData

/**
 * Checks if url is artifact url.
 * @param shouldSkipMatched if true will pass only non-matched, else will pass only matched
 */
class ApkMirrorArtifactsFilter(private val shouldSkipMatched: Boolean)
  extends FilterFunction[ScheduledUrlData]{


  override def filter(value: ScheduledUrlData): Boolean = {
    val isArtifact = Utils.tryUrl(value.url)
      .fold({_ => false}, { url =>
        //I didn't found any other paths
        url.getPath.equals(ApkMirrorArtifactsFilter.ArtifactPath)
      })

    isArtifact != shouldSkipMatched
  }
}

object ApkMirrorArtifactsFilter {
  private val ArtifactPath = "/wp-content/themes/APKMirror/download.php"
  def apply(shouldSkipMatched: Boolean): ApkMirrorArtifactsFilter =
    new ApkMirrorArtifactsFilter(shouldSkipMatched)
}
