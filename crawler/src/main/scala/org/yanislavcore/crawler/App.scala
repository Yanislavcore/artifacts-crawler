package org.yanislavcore.crawler

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.slf4j.LoggerFactory
import org.yanislavcore.common.data.ScheduledUrlData
import org.yanislavcore.common.service.HttpFetchService
import org.yanislavcore.common.stream.{AsyncUrlFetchFunction, FetchFailureSerializationSchema, FetchSuccessSplitter, ScheduledUrlDeserializationSchema}
import org.yanislavcore.crawler.FlinkHelpers._
import org.yanislavcore.crawler.service.{AerospikeMetGloballyRepository, CacheMetLocallyRepository}
import org.yanislavcore.crawler.stream._
import pureconfig._

object App {

  private val log = LoggerFactory.getLogger(classOf[App])

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val configPath = ParameterTool.fromArgs(args).get("config-file")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val cfg = parseConfig(configPath)

    //Reading from Kafka
    val scheduledUrlsStream = kafkaSource(env, cfg)
      .name("ScheduledUrlsSource")

    //Fetching
    val fetchedStream = AsyncDataStream.unorderedWait(
      scheduledUrlsStream,
      AsyncUrlFetchFunction(HttpFetchService(cfg.fetcher.threads, cfg.fetcher.timeout)),
      cfg.fetcher.timeout.toMillis,
      TimeUnit.MILLISECONDS
    )
      .name("HtmlFetcher")
      .process(FetchSuccessSplitter())
      .name("FetchSuccessSplitter")

    //Unsuccessful
    fetchedStream
      .getSideOutput(FetchSuccessSplitter.FetchFailedTag)
      .addSink(kafkaSink(cfg, cfg.quarantineUrlsTopic, FetchFailureSerializationSchema(cfg.quarantineUrlsTopic)))

    //Filtering and crawling successful urls
    val notMetLocallyUrls = fetchedStream
      .flatMap(UrlsCollector(cfg))
      .name("UrlsCollect")
      //Filtering already met locally
      .filter(LocalCacheFilter(CacheMetLocallyRepository(cfg)))
      .name("LocalCacheFilter")

    //Filtering on cluster
    val notMetUrls = AsyncDataStream.unorderedWait(
      notMetLocallyUrls,
      MetGloballyChecker(AerospikeMetGloballyRepository(cfg)),
      cfg.clusterCache.timeout.toMillis,
      TimeUnit.MILLISECONDS,
    )
      .name("ClusterMetMetaEnricher")
      .filter { value => !value._2 }
      .name("ClusterMetMetaFilter")
      .map { value => value._1 }
      .name("ClusterMetMetaEraser")
      //Splitting on artifacts and not artifacts
      .process(ApkMirrorArtifactsSplitter())

    // ===== Non-artifacts =====
    notMetUrls
      .addSink(kafkaSink(cfg, cfg.urlsTopic, ScheduledUrlSerializationSchema(cfg.urlsTopic)))
      .name("NonArtifactsSink")

    // ===== Artifacts =====

    notMetUrls
      .getSideOutput(ArtifactTag)
      .addSink(kafkaSink(cfg, cfg.artifactsTopic, ScheduledUrlSerializationSchema(cfg.artifactsTopic)))
      .name("ArtifactsSink")


    env.execute("Artifacts Fetcher")
  }


  def kafkaSource(env: StreamExecutionEnvironment, cfg: CrawlerConfig): DataStream[ScheduledUrlData] = {
    val props = getKafkaProps(cfg)
    val schema = new ScheduledUrlDeserializationSchema()
    val source = new FlinkKafkaConsumer(cfg.urlsTopic, schema, props)
    env.addSource(source)(schema.getProducedType)
  }

  @throws[Exception]
  def parseConfig(cfgPath: String): CrawlerConfig = {
    import pureconfig.generic.auto._
    val loader = if (cfgPath != null) {
      log.info("Loading config from {}", cfgPath)
      ConfigSource.file(cfgPath)
    } else {
      log.warn("Using classpath config")
      ConfigSource.default
    }
    loader.load[CrawlerConfig].fold(
      e => throw new RuntimeException("Failed to parse config. Failures: " ++ e.prettyPrint()),
      cfg => cfg
    )
  }

  def kafkaSink[T](cfg: CrawlerConfig,
                   topic: String,
                   schema: KafkaSerializationSchema[T]): FlinkKafkaProducer[T] = {
    val props = getKafkaProps(cfg)
    val producer = new FlinkKafkaProducer(
      topic,
      schema,
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
