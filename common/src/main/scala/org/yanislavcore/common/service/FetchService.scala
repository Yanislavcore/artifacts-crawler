package org.yanislavcore.common.service

import java.util.concurrent.Executor

import org.yanislavcore.common.data.FetchSuccessData

import scala.concurrent.Future

trait FetchService extends Serializable {
  def fetch(url: String)(implicit ex: Executor): Future[FetchSuccessData]
}
