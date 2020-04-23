package org.yanislavcore.crawler.stream

import java.io.ByteArrayInputStream
import java.net.URL

import com.google.common.net.{InternetDomainName, MediaType}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass
import org.apache.flink.util.Collector
import org.jsoup.Jsoup
import org.jsoup.select.Elements
import org.yanislavcore.common.Utils
import org.yanislavcore.common.data.{FetchedData, ScheduledUrlData}
import org.yanislavcore.crawler.CrawlerConfig

import scala.util.Try

class UrlsCollector(cfg: CrawlerConfig) extends FlatMapFunction[(ScheduledUrlData, FetchedData), ScheduledUrlData] {
  private val ignoredExtensions = cfg.ignoredExtensions.toSet

  override def flatMap(value: (ScheduledUrlData, FetchedData), out: Collector[ScheduledUrlData]): Unit = {
    //noinspection UnstableApiUsage
    if (!MediaType.parse(value._2.contentType).equals(MediaType.HTML_UTF_8)) {
      return
    }
    val originUrl = new URL(value._1.url)
    val ignoreExternal = value._1.ignoreExternalUrls
    val originHost = originUrl.getHost
    val baseUrl = if (originUrl.getPort != -1) {
      originUrl.getProtocol + "://" + originHost + ":" + originUrl.getDefaultPort
    } else {
      originUrl.getProtocol + "://" + originHost
    }

    val links = parseElements(value, baseUrl)

    //noinspection UnstableApiUsage
    val originPublicDomain = InternetDomainName.from(originHost).topPrivateDomain().toString
    val dottedOriginHost = "." + originPublicDomain

    links.forEach { el =>
      val newUrl = el.attr("abs:href")
      if (newUrl != null && newUrl != "") {
        //Skipping errors
        Utils.tryUrl(newUrl).foreach { u =>
          val newDomain = u.getHost
          //Domain filtering
          val condition = !isIgnoredExtension(u) &&
            (!ignoreExternal ||
              newDomain.equals(originPublicDomain) ||
              newDomain.equals(originHost) ||
              newDomain.endsWith(dottedOriginHost))
          if (condition) {
            //collecting
            out.collect(ScheduledUrlData(newUrl, value._1.ignoreExternalUrls))
          }
        }
      }
    }
  }

  private def isIgnoredExtension(url: URL): Boolean = {
    val split = url.getPath.split('.')
    ignoredExtensions.contains(split.last)
  }

  private def parseElements(value: (ScheduledUrlData, FetchedData), baseUrl: String): Elements = {
    //Skipping if not valid HTML
    Try {
      val data = value._2.body
      Utils.withResources(new ByteArrayInputStream(data)) { stream =>
        Jsoup.parse(stream, "utf-8", baseUrl).select("a[href]")
      }
    }
      .getOrElse(null)
  }
}

object UrlsCollector {
  def getProducedType: TypeInformation[ScheduledUrlData] = {
    getForClass(classOf[ScheduledUrlData])
  }

  def apply(cfg: CrawlerConfig): UrlsCollector = new UrlsCollector(cfg: CrawlerConfig)
}
