package org.yanislavcore.crawler.stream

import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yanislavcore.common.data.ScheduledUrlData
import org.yanislavcore.crawler.FlinkHelpers

import scala.collection.JavaConverters._

class ApkMirrorArtifactsSplitterTest extends AnyFlatSpec with Matchers with MockFactory {
  "ApkMirrorArtifactsSplitter" should "pass anything through except artifacts" in {
    val s = ApkMirrorArtifactsSplitter()
    val harness = ProcessFunctionTestHarnesses.forProcessFunction(s)


    val expected = Set(
      "http://domain.com/some_local.html",
      "http://domain.com/same_domain.html",
      "http://wwwasd.domain.com/sub_domain.html",
      "http://another.com/another_domain.html",
      "http://wwwasd.another.com/another_sub_domain.html",
      "http://wwwasd.domain.com/ignored.png"
    ).map { s =>
      ScheduledUrlData(s, ignoreExternalUrls = true)
    }
    expected.foreach { s =>
      harness.processElement(s, 0)
    }

    harness.extractOutputValues().asScala.toSet should be(expected)
  }

  "ApkMirrorArtifactsSplitter" should "pass artifact to side output" in {
    val s = ApkMirrorArtifactsSplitter()
    val harness = ProcessFunctionTestHarnesses.forProcessFunction(s)


    val expected = Set(
      "http://domain.com/some_local.html",
      "http://domain.com/same_domain.html",
      "http://wwwasd.domain.com/sub_domain.html",
      "http://another.com/another_domain.html",
      "http://wwwasd.another.com/another_sub_domain.html",
      "http://wwwasd.domain.com/ignored.png"
    ).map { s =>
      ScheduledUrlData(s, ignoreExternalUrls = true)
    }
    expected.foreach { s =>
      harness.processElement(s, 0)
    }
    val artifact = ScheduledUrlData("http://wwwasd.domain.com/wp-content/themes/APKMirror/download.php", ignoreExternalUrls = true)
    harness.processElement(
      artifact,
      0
    )

    harness.extractOutputValues().asScala.toSet should be(expected)

    val side = harness.getSideOutput(FlinkHelpers.ArtifactTag).asScala
    side.size should be(1)
    side.last.getValue should be(artifact)

  }
}
