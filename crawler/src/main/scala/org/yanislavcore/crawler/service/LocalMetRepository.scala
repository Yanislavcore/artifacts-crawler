package org.yanislavcore.crawler.service

trait LocalMetRepository extends Serializable {
  /**
   * Check if URL is already met and puts it again
   */
  def checkAndPut(url: String): Boolean
}
