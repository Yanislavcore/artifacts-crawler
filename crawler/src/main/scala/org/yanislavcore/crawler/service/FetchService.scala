package org.yanislavcore.crawler.service

import java.util.concurrent.Executor

import org.yanislavcore.crawler.data.FetchSuccess

import scala.concurrent.{ExecutionContext, Future, Promise}

trait FetchService extends Serializable {
  def fetch(url: String)(implicit ex: Executor): Future[FetchSuccess]
}
