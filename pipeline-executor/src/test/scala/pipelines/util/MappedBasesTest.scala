package org.gc.pipelines.util

import org.scalatest._
import org.scalatest.{Matchers, BeforeAndAfterAll}

import java.io.File

class MappedBasesTestSuite
    extends FunSuite
    with BeforeAndAfterAll
    with GivenWhenThen
    with Matchers {
  test("count bases") {
    Given("a bam file")

    val bam =
      new File(getClass.getResource("/tutorial_8017/papa.bam.sorted").getFile)

    val tmp = fileutils.openFileWriter { wr =>
      wr.write("chr19\t0\t34351111")
    }._1

    MappedBases.countBases(bam, 0, 0, Some(tmp.getAbsolutePath)) shouldBe ((1510000L,
                                                                            2205))

  }
}
