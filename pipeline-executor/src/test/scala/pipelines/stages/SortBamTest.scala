package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import java.io.File

import org.gc.pipelines.model._

class SortBamTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("SortBam should sort a bam by coordinate and create and idnex") {
    new Fixture {

      val result = withTaskSystem(testConfig) { implicit ts =>
        val input = Bam(await(SharedFile(bam, "some.bam")))

        val future =
          BWAAlignment.sortByCoordinateAndIndex(input)(ResourceRequest(1, 500))
        val bamFile = await(await(future).bam.file)
        val baiFile = await(await(future).bai.file)
        (bamFile, baiFile)
      }

      val (sortedBam, bai) = result.get
      sortedBam.canRead shouldBe true
      bai.canRead shouldBe true

      getSortOrder(sortedBam) shouldBe "coordinate"

      recordsInBamFile(sortedBam) shouldBe 10000

    }
  }

  trait Fixture {

    val project = Project("someProject")
    val sampleId = SampleId("someSampleId")
    val runId = RunId("someRunId")
    val lane = Lane(1)

    val bam = new File(getClass.getResource("/tutorial_8017/papa.bam").getFile)

    val (testConfig, basePath) = makeTestConfig
  }
}
