package org.yanislavcore.fetcher

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.yanislavcore.common.data.{FailedUrlData, FetchedData, ScheduledUrlData, UrlProcessFailure}
import org.yanislavcore.common.stream.{AsyncUrlFetchFunction, FetchSuccessSplitter}
import org.yanislavcore.fetcher.data.ArchiveMetadata
import org.yanislavcore.fetcher.stream.{AsyncDataUnpackerFunction, DataUnpackerResultSplitter}

object FlinkHelpers {
  /** Type Information, required by Flink */
  implicit val fetchedDataTypeInfo: TypeInformation[(ScheduledUrlData, Either[UrlProcessFailure, FetchedData])] =
    AsyncUrlFetchFunction.getProducedType
  implicit val successFetchTypeInfo: TypeInformation[(ScheduledUrlData, FetchedData)] =
    TypeExtractor.getForClass(classOf[(ScheduledUrlData, FetchedData)])
  implicit val fetchFailureTypeInfo: TypeInformation[FailedUrlData] =
    FetchSuccessSplitter.getProducedSideType
  implicit val dataUnpackerTypeInfo: TypeInformation[Either[(ScheduledUrlData, UrlProcessFailure), ArchiveMetadata]] =
    AsyncDataUnpackerFunction.getProducedType
  implicit val unpackedMetaTypeInfo: TypeInformation[ArchiveMetadata] =
    DataUnpackerResultSplitter.getProducedType
}
