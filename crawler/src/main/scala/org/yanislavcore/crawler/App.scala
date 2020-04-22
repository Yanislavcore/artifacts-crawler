package org.yanislavcore.crawler

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.yanislavcore.crawler.data.{FetchFailure, FetchSuccess, ScheduledUrlData}
import org.yanislavcore.crawler.service.{AerospikeClusterMetRepository, CacheLocalMetRepository, HttpFetchService}
import org.yanislavcore.crawler.stream._
import pureconfig._

object App {

  /** Type Information, required by Flink */
  private implicit val scheduledUrlTypeInfo: TypeInformation[ScheduledUrlData] =
    ScheduledUrlDeserializer.getProducedType
  private implicit val fetchedDataTypeInfo: TypeInformation[(ScheduledUrlData, Either[FetchFailure, FetchSuccess])] =
    UrlFetchMapper.getProducedType
  private implicit val successFetchTypeInfo: TypeInformation[(ScheduledUrlData, FetchSuccess)] =
    TypeExtractor.getForClass(classOf[(ScheduledUrlData, FetchSuccess)])
  private implicit val stringTypeInfo: TypeInformation[String] =
    TypeExtractor.getForClass(classOf[String])
  private implicit val clusterCheckerTypeInfo: TypeInformation[(ScheduledUrlData, Boolean)] =
    ClusterCacheChecker.getProducedType

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val cfg = getConfig

    //Reading from Kafka
    val scheduledUrlsStream = kafkaSource(env, cfg)
      .name("ScheduledUrlsSource")
      .map(ScheduledUrlDeserializer())
      .name("parser")

    //Fetching
    val fetchedStream = AsyncDataStream.unorderedWait(
      scheduledUrlsStream,
      UrlFetchMapper(HttpFetchService),
      cfg.fetcher.timeout.toMillis,
      TimeUnit.MILLISECONDS,
    )
      .name("HtmlFetcher")

    //Filtering and crawling urls
    val notMetLocallyUrls = fetchedStream
      .filter { value =>
        val f = value._2
        f.isRight && f.getOrElse(null).code / 100 == 2
      }
      .name("SuccessFilter")
      .map { value =>
        //Never will be null
        (value._1, value._2.getOrElse(null))
      }
      .name("FailureMetaEraser")
      .flatMap(UrlsCollector(cfg))
      .name("UrlsCollect")
      //Filtering already met
      .filter(LocalCacheFilter(CacheLocalMetRepository))
      .name("LocalCacheFilter")

    //Filtering on cluster
    val notMetUrls = AsyncDataStream.unorderedWait(
      notMetLocallyUrls,
      ClusterCacheChecker(AerospikeClusterMetRepository),
      cfg.clusterCache.timeout.toMillis,
      TimeUnit.MILLISECONDS,
    )
      .name("ClusterMetMetaEnricher")
      .filter { value => !value._2 }
      .name("ClusterMetMetaFilter")
      .map { value => value._1 }
      .name("ClusterMetMetaEraser")

    // ===== Non-artifacts =====
    notMetUrls
      .filter(ApkMirrorArtifactsFilter(shouldSkipMatched = true))
      .name("NonArtifactsFilter")
      .addSink(kafkaSink(cfg, cfg.urlsTopic))
      .name("NonArtifactsSink")

    // ===== Artifacts =====

    notMetUrls
      .filter(ApkMirrorArtifactsFilter(shouldSkipMatched = false))
      .name("ArtifactsFilter")
      .addSink(kafkaSink(cfg, cfg.artifactsTopic))
      .name("ArtifactsSink")


    env.execute("Artifacts crawler")
  }


  def kafkaSource(env: StreamExecutionEnvironment, cfg: CrawlerConfig): DataStream[String] = {
    val props = getKafkaProps(cfg)
    val schema = new SimpleStringSchema()
    val source = new FlinkKafkaConsumer(cfg.urlsTopic, schema, props)
    env.addSource(source)(schema.getProducedType)
      .name("ScheduledUrlConsumer")
  }

  @throws[Exception]
  def getConfig: CrawlerConfig = {
    import pureconfig.generic.auto._
    ConfigSource.default.load[CrawlerConfig].fold(
      e => throw new RuntimeException("Failed to parse config. Failures: " ++ e.prettyPrint()),
      cfg => cfg
    )
  }

  def kafkaSink(cfg: CrawlerConfig, topic: String): FlinkKafkaProducer[ScheduledUrlData] = {
    val props = getKafkaProps(cfg)
    val producer = new FlinkKafkaProducer(
      topic,
      ScheduledUrlSerializationSchema(topic),
      props,
      FlinkKafkaProducer.Semantic.AT_LEAST_ONCE,
      cfg.maxProducers
    )
    producer.setWriteTimestampToKafka(true)
    producer
  }

  def getKafkaProps(cfg: CrawlerConfig): Properties = {
    val props = new Properties()
    //Bug in scala with JDK 9+ and putAll(): https://github.com/scala/bug/issues/10418 , workaround:
    cfg.kafkaOptions.foreach { case (key, value) => props.put(key, value) }
    props
  }

}
