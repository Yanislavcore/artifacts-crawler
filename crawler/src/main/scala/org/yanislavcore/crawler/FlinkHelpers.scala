package org.yanislavcore.crawler

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.yanislavcore.common.data.{FailedUrlData, FetchedData, ScheduledUrlData, UrlProcessFailure}
import org.yanislavcore.common.stream.{AsyncUrlFetchFunction, FetchSuccessSplitter}
import org.yanislavcore.crawler.stream.MetGloballyChecker

object FlinkHelpers {
  /** Type Information, required by Flink */
  implicit val scheduledUrlTypeInfo: TypeInformation[ScheduledUrlData] =
    TypeExtractor.getForClass(classOf[ScheduledUrlData])
  implicit val fetchedDataTypeInfo: TypeInformation[(ScheduledUrlData, Either[UrlProcessFailure, FetchedData])] =
    AsyncUrlFetchFunction.getProducedType
  implicit val successFetchTypeInfo: TypeInformation[(ScheduledUrlData, FetchedData)] =
    TypeExtractor.getForClass(classOf[(ScheduledUrlData, FetchedData)])
  implicit val stringTypeInfo: TypeInformation[String] =
    TypeExtractor.getForClass(classOf[String])
  implicit val clusterCheckerTypeInfo: TypeInformation[(ScheduledUrlData, Boolean)] =
    MetGloballyChecker.getProducedType
  implicit val fetchFailureTypeInfo: TypeInformation[FailedUrlData] =
    FetchSuccessSplitter.getProducedSideType

  val ArtifactTag: OutputTag[ScheduledUrlData] = OutputTag[ScheduledUrlData]("ArtifactStream")
}
