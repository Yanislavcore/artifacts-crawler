package org.yanislavcore.crawler.stream

import com.google.common.net.MediaType
import org.apache.flink.util.Collector
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yanislavcore.common.data.{FetchedData, ScheduledUrlData}
import org.yanislavcore.crawler.CrawlerConfig

//noinspection UnstableApiUsage
class UrlsCollectorTest extends AnyFlatSpec with Matchers with MockFactory {
  private val html =
    """
      |<html>
      |<a href="some_local.html"></a>
      |<a href="http://domain.com/same_domain.html"></a>
      |<a href="http://wwwasd.domain.com/sub_domain.html"></a>
      |<a href="http://another.com/another_domain.html"></a>
      |<a href="http://wwwasd.another.com/another_sub_domain.html"></a>
      |<a href="http://wwwasd.domain.com/ignored.png"></a>
      |<img src="http://wwwasd.domain.com/ignored_img.html">
      |</html>
      |""".stripMargin

  "UrlsCollector" should "ignore png extension and other domains if set so" in {
    val s = UrlsCollector(CrawlerConfig(null, null, null, 0, null, null, List("png"), null, null))
    val collectorMock = mock[Collector[ScheduledUrlData]]
    inAnyOrder {
      List(
        "http://domain.com/some_local.html",
        "http://domain.com/same_domain.html",
        "http://wwwasd.domain.com/sub_domain.html"
      )
        .foreach { s =>
          (collectorMock.collect _).expects(ScheduledUrlData(s, ignoreExternalUrls = true))
        }
    }

    s.flatMap((
      ScheduledUrlData("http://domain.com/", ignoreExternalUrls = true),
      FetchedData(200, html.getBytes, MediaType.HTML_UTF_8.toString, "http://domain.com/")),
      collectorMock)
  }

  it should "include png extension and other domains if set so" in {
    val s = UrlsCollector(CrawlerConfig(null, null, null, 0, null, null, List(), null, null))
    val collectorMock = mock[Collector[ScheduledUrlData]]
    inAnyOrder {
      List(
        "http://domain.com/some_local.html",
        "http://domain.com/same_domain.html",
        "http://wwwasd.domain.com/sub_domain.html",
        "http://another.com/another_domain.html",
        "http://wwwasd.another.com/another_sub_domain.html",
        "http://wwwasd.domain.com/ignored.png"
      )
        .foreach { s =>
          (collectorMock.collect _).expects(ScheduledUrlData(s, ignoreExternalUrls = false))
        }
    }

    s.flatMap((
      ScheduledUrlData("http://domain.com/", ignoreExternalUrls = false),
      FetchedData(200, html.getBytes, MediaType.HTML_UTF_8.toString, "http://domain.com/")),
      collectorMock)
  }

  it should "include png only extension if set so" in {
    val s = UrlsCollector(CrawlerConfig(null, null, null, 0, null, null, List("html"), null, null))
    val collectorMock = mock[Collector[ScheduledUrlData]]
    inAnyOrder {
      List(
        "http://wwwasd.domain.com/ignored.png"
      )
        .foreach { s =>
          (collectorMock.collect _).expects(ScheduledUrlData(s, ignoreExternalUrls = true))
        }
    }

    s.flatMap((
      ScheduledUrlData("http://domain.com/", ignoreExternalUrls = true),
      FetchedData(200, html.getBytes, MediaType.HTML_UTF_8.toString, "http://domain.com/")),
      collectorMock)
  }
}
