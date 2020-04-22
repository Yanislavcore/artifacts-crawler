package org.yanislavcore.crawler.service

import scala.concurrent.Future

trait MetGloballyRepository extends Serializable {
  /**
   * Check if URL is already met and puts it again
   */
  def checkAndPut(url: String): Future[Boolean]
}
