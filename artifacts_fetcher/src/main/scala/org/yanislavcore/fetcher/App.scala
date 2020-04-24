package org.yanislavcore.fetcher

import java.util.Properties
import java.util.concurrent.TimeUnit

import javax.annotation.Nullable
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.slf4j.LoggerFactory
import org.yanislavcore.common.data.ScheduledUrlData
import org.yanislavcore.common.service.HttpFetchService
import org.yanislavcore.common.stream.{AsyncUrlFetchFunction, FetchFailureSerializationSchema, FetchSuccessSplitter, ScheduledUrlDeserializationSchema}
import org.yanislavcore.fetcher.FlinkHelpers._
import org.yanislavcore.fetcher.data.ArchiveMetadata
import org.yanislavcore.fetcher.service.{FileWriterServiceImpl, IoExecutorServiceHolder, UnzipServiceImpl}
import org.yanislavcore.fetcher.stream.{ApkContentTypeSplitter, AsyncDataUnpackerFunction, DataUnpackerResultSplitter, LoggingSink}
import pureconfig.ConfigSource

object App {

  private val log = LoggerFactory.getLogger(classOf[App])

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val configPath = ParameterTool.fromArgs(args).get("config-file")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val cfg = parseConfig(configPath)

    val source = kafkaSource(env, cfg)
      .name("ArtifactUrlsConsumer")

    val fetchedStream = AsyncDataStream.unorderedWait(
      source,
      AsyncUrlFetchFunction(HttpFetchService(cfg.fetcher.threads, cfg.fetcher.timeout)),
      cfg.fetcher.timeout.toMillis,
      TimeUnit.MILLISECONDS
    )
      .name("DataFetcher")
      .process(FetchSuccessSplitter())
      .name("FetchSuccessSplitter")
      .process(ApkContentTypeSplitter())
      .name("ApkContentTypeSplitter")

    //Unpacking and storing successfully fetched artifacts
    val unpackedStream = AsyncDataStream.unorderedWait(
      fetchedStream,
      AsyncDataUnpackerFunction(cfg, FileWriterServiceImpl, UnzipServiceImpl, IoExecutorServiceHolder(cfg)),
      cfg.unpacker.timeout.toMillis,
      TimeUnit.MILLISECONDS
    )
      .name("AsyncDataUnpacker")
      .process(DataUnpackerResultSplitter())
      .name("DataUnpackResultSplitter")

    //Failed to fetch
    val failedSink = kafkaSink(cfg, cfg.quarantineArtifactsTopic, FetchFailureSerializationSchema(cfg.quarantineArtifactsTopic))
    fetchedStream
      .getSideOutput(FetchSuccessSplitter.FetchFailedTag)
      .addSink(failedSink)
      .name("FetchFailedArtifacts")

    //Failed to unpack
    unpackedStream
      .getSideOutput(DataUnpackerResultSplitter.UnpackFailedTag)
      .addSink(failedSink)
      .name("UnpackFailedArtifacts")

    //TODO Just logs metadata. You need to setup your own metadata sink
    unpackedStream
      .addSink(LoggingSink[ArchiveMetadata]())
      .name("ResultsLogArtifacts")

    env.execute("Artifacts crawler")
  }

  def kafkaSource(env: StreamExecutionEnvironment, cfg: ArtifactsFetcherConfig): DataStream[ScheduledUrlData] = {
    val props = getKafkaProps(cfg)
    val schema = new ScheduledUrlDeserializationSchema()
    val source = new FlinkKafkaConsumer(cfg.artifactsTopic, schema, props)
    env.addSource(source)(schema.getProducedType)
  }

  def getKafkaProps(cfg: ArtifactsFetcherConfig): Properties = {
    val props = new Properties()
    //Bug in scala with JDK 9+ and putAll(): https://github.com/scala/bug/issues/10418 , workaround:
    cfg.kafkaOptions.foreach { case (key, value) => props.put(key, value) }
    props
  }

  @throws[Exception]
  def parseConfig(@Nullable cfgPath: String): ArtifactsFetcherConfig = {
    import pureconfig.generic.auto._
    val loader = if (cfgPath != null) {
      log.info("Loading config from {}", cfgPath)
      ConfigSource.file(cfgPath)
    } else {
      log.warn("Using classpath config")
      ConfigSource.default
    }
    loader.load[ArtifactsFetcherConfig].fold(
      e => throw new RuntimeException("Failed to parse config. Failures: " ++ e.prettyPrint()),
      cfg => cfg
    )
  }

  def kafkaSink[T](cfg: ArtifactsFetcherConfig,
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
}
