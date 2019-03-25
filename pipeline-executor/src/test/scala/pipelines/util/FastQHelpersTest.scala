package org.gc.pipelines.util

import org.scalatest._
import org.scalatest.{Matchers, BeforeAndAfterAll}

import java.io.File

class FastQHelpersTestSuite
    extends FunSuite
    with BeforeAndAfterAll
    with GivenWhenThen
    with Matchers {
  test("split fastq.gz") {
    Given("a fq file with 5k reads")
    val originalFile =
      new File(getClass.getResource("/tutorial_8017/papa.read1.bgzip").getFile)
    When("splitting by 999 reads")
    val splits = FastQHelpers.splitFastQ(originalFile, 999L)
    Then("the number of splits should be 10")
    splits.size shouldBe 6
    And(" no data should be lost")
    splits
      .map(f => FastQHelpers.getNumberOfReads(f._1))
      .sum shouldBe FastQHelpers
      .getNumberOfReads(originalFile)
    And("concatenating the splits should equal the original")
    fileutils
      .openSource(originalFile) { source1 =>
        splits.foreach {
          case (fp, _) =>
            fileutils.openSource(fp) { source2 =>
              while (source2.hasNext) {
                source1.next shouldBe source2.next
              }
            }
        }
      }
    And("each partition should be ok to gunzip")
    splits.foreach {
      case (f, _) =>
        fileutils.openSource(f) { s =>
          s.mkString
        }
    }
    And("each partition should have the correct size")
    splits.dropRight(1).foreach {
      case (f, _) =>
        FastQHelpers.getNumberOfReads(f) shouldBe 999
    }
    FastQHelpers.getNumberOfReads(splits.last._1) shouldBe 5

    And("each partition should have the correct computed size")
    splits.map(_._2).toList shouldBe List(999, 999, 999, 999, 999, 5)

  }
}
