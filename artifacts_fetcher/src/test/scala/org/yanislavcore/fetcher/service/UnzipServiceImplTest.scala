package org.yanislavcore.fetcher.service

import java.io.File
import java.nio.file.{Files, Paths}

import com.google.common.io.Resources
import net.lingala.zip4j.exception.ZipException
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

//noinspection UnstableApiUsage
class UnzipServiceImplTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val TestsTmpDirTarget = "/tmp/artifacts_crawler_tests/origin/"
  private val TestsTmpDirSource = "/tmp/artifacts_crawler_tests/unpacked/"

  override protected def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File(TestsTmpDirTarget))
    FileUtils.deleteDirectory(new File(TestsTmpDirSource))
    val validZip = Resources.getResource("testZips/valid.zip")
    val nonValidZip = Resources.getResource("testZips/nonValid.zip")
    Files.createDirectories(Paths.get(TestsTmpDirSource))
    Files.createDirectories(Paths.get(TestsTmpDirTarget))
    Files.write(Paths.get(TestsTmpDirSource, "valid.zip"), Resources.toByteArray(validZip))
    Files.write(Paths.get(TestsTmpDirSource, "nonValid.zip"), Resources.toByteArray(nonValidZip))
  }

  override protected def afterEach(): Unit = {
    FileUtils.deleteDirectory(new File(TestsTmpDirTarget))
    FileUtils.deleteDirectory(new File(TestsTmpDirSource))
  }

  "UnzipServiceImpl" should "unpack zip successfully and return meta" in {
    UnzipServiceImpl.unzip(TestsTmpDirSource + "/valid.zip", TestsTmpDirTarget)
    Files.exists(Paths.get(TestsTmpDirTarget, "inside/inside_file")) should be(true)
    Files.exists(Paths.get(TestsTmpDirTarget, "upper")) should be(true)
  }

  it should "fail on invalid zip archive" in {
    a[ZipException] should be thrownBy {
      UnzipServiceImpl.unzip(TestsTmpDirSource + "/nonValid.zip", TestsTmpDirTarget)
    }
  }
}
