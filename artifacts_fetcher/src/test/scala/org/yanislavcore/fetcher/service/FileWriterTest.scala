package org.yanislavcore.fetcher.service

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FileWriterTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  private val TestsTmpDirTarget = "/tmp/artifacts_crawler_tests/writer/"

  override protected def beforeEach(): Unit = {
    Files.createDirectories(Paths.get(TestsTmpDirTarget))
  }

  override protected def afterEach(): Unit = {
    FileUtils.deleteDirectory(new File(TestsTmpDirTarget))
  }

  "FileWriterServiceImpl" should "write file successfully" in {
    val data = "sometestdata"
    FileWriterServiceImpl.writeFile(data.getBytes("utf-8"), TestsTmpDirTarget, "test.txt")
    val result = Files.readAllLines(Paths.get(TestsTmpDirTarget, "test.txt"))
    result.size() should be(1)
    result.get(0) should be(data)
  }

  it should "create dir if not exist" in {
    FileWriterServiceImpl.writeFile("sdsfds".getBytes("utf-8"), TestsTmpDirTarget + "/test", "test.txt")
    Files.isDirectory(Paths.get(TestsTmpDirTarget, "test")) should be(true)
  }

}
