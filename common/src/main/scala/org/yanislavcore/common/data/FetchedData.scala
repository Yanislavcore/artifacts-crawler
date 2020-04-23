package org.yanislavcore.common.data

case class FetchedData(code: Int,
                       body: Array[Byte],
                       contentType: String,
                       url: String)
