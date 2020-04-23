package org.yanislavcore.fetcher

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.yanislavcore.common.data.{FailedUrlData, FetchFailureData, FetchSuccessData, ScheduledUrlData}
import org.yanislavcore.common.stream.{AsyncUrlFetchFunction, FetchSuccessSplitter}

object FlinkHelpers {
  /** Type Information, required by Flink */
  implicit val fetchedDataTypeInfo: TypeInformation[(ScheduledUrlData, Either[FetchFailureData, FetchSuccessData])] =
    AsyncUrlFetchFunction.getProducedType
  implicit val successFetchTypeInfo: TypeInformation[(ScheduledUrlData, FetchSuccessData)] =
    TypeExtractor.getForClass(classOf[(ScheduledUrlData, FetchSuccessData)])
  implicit val fetchFailureTypeInfo: TypeInformation[FailedUrlData] =
    FetchSuccessSplitter.getProducedSideType

}
