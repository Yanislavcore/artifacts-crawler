package org.yanislavcore.crawler

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.yanislavcore.crawler.data.{FailedUrlData, FetchFailure, FetchSuccess, ScheduledUrlData}
import org.yanislavcore.crawler.stream.{FetchSuccessSplitter, MetGloballyChecker, ScheduledUrlDeserializer, UrlFetchMapper}

object FlinkHelpers {
  /** Type Information, required by Flink */
  implicit val scheduledUrlTypeInfo: TypeInformation[ScheduledUrlData] =
    ScheduledUrlDeserializer.getProducedType
  implicit val fetchedDataTypeInfo: TypeInformation[(ScheduledUrlData, Either[FetchFailure, FetchSuccess])] =
    UrlFetchMapper.getProducedType
  implicit val successFetchTypeInfo: TypeInformation[(ScheduledUrlData, FetchSuccess)] =
    TypeExtractor.getForClass(classOf[(ScheduledUrlData, FetchSuccess)])
  implicit val stringTypeInfo: TypeInformation[String] =
    TypeExtractor.getForClass(classOf[String])
  implicit val clusterCheckerTypeInfo: TypeInformation[(ScheduledUrlData, Boolean)] =
    MetGloballyChecker.getProducedType
  implicit val fetchFailureTypeInfo: TypeInformation[FailedUrlData] =
    FetchSuccessSplitter.getProducedSideType

  val FetchFailedTag: OutputTag[FailedUrlData] =
    OutputTag[FailedUrlData]("fetchedFailedStream")
  val ArtifactTag: OutputTag[ScheduledUrlData] = OutputTag[ScheduledUrlData]("ArtifactStream")
}
