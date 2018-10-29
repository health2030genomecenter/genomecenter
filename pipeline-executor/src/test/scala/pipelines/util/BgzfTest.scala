package org.gc.pipelines.util

import org.scalatest._
import org.scalatest.{Matchers, BeforeAndAfterAll}

import java.io.File

class BgzfTestSuite
    extends FunSuite
    with BeforeAndAfterAll
    with GivenWhenThen
    with Matchers {
  test("redistribute bgzf gzip blocks") {
    Given("a bgzf file")
    val originalFile =
      new File(getClass.getResource("/tutorial_8017/papa.read1.bgzip").getFile)
    val totalSize = originalFile.length
    When("partitioning to single gzip blocks")
    val partitions = Bgzf.partition(originalFile, 0)
    Then("the number of gzip blocks should be correct")
    partitions.size shouldBe 30
    And(" no data should be lost")
    partitions.map(_.length).sum shouldBe totalSize
    And("concatenating the partitions should equal the original")
    fileutils
      .openFileInputStream(originalFile) { is1 =>
        partitions.foreach { fp =>
          fileutils.openFileInputStream(fp) { is =>
            var c = is.read
            while (c != -1) {
              c shouldBe is1.read
              c = is.read
            }
          }
        }
      }
    And("each partition should be ok to gunzip")
    partitions.foreach { f =>
      fileutils.openSource(f) { s =>
        s.mkString
      }
    }

  }
}
