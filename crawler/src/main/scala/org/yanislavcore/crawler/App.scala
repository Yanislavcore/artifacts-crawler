package org.yanislavcore.crawler

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.yanislavcore.common.service.HttpFetchService
import org.yanislavcore.common.stream.{AsyncUrlFetchFunction, FetchSuccessSplitter}
import org.yanislavcore.crawler.FlinkHelpers._
import org.yanislavcore.crawler.service.{AerospikeMetGloballyRepository, CacheMetLocallyRepository}
import org.yanislavcore.crawler.stream._
import pureconfig._

object App {

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
      AsyncUrlFetchFunction(HttpFetchService),
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
      .filter(LocalCacheFilter(CacheMetLocallyRepository))
      .name("LocalCacheFilter")

    //Filtering on cluster
    val notMetUrls = AsyncDataStream.unorderedWait(
      notMetLocallyUrls,
      MetGloballyChecker(AerospikeMetGloballyRepository),
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
