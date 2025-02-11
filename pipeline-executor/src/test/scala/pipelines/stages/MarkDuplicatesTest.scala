package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import java.io.File

import org.gc.pipelines.model._
import org.gc.pipelines.util.StableSet

class MarkDuplicatesTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("Merge-then-markduplicates step should merge and sort bams per sample") {
    new Fixture {

      val result = withTaskSystem(testConfig) { implicit ts =>
        val input =
          BamsWithSampleMetadata(
            bams = StableSet(Bam(await(SharedFile(bam, "some.bam")))),
            project = project,
            sampleId = sampleId
          )

        val future =
          BWAAlignment.mergeAndMarkDuplicate(input)(ResourceRequest(1, 500))
        val markDupResult = await(future)
        val bamFile = await(markDupResult.bam.bam.file.file)
        (markDupResult, bamFile)
      }

      val (alignedSample, localBam) = result.get
      alignedSample.bam.project shouldBe project
      localBam.canRead shouldBe true
      new File(localBam.getParentFile, localBam.getName + ".stderr").canRead shouldBe true

      recordsInBamFile(localBam) shouldBe 10000

    }
  }

  trait Fixture {

    val project = Project("someProject")
    val sampleId = SampleId("someSampleId")
    val runId = RunId("someRunId")
    val lane = Lane(1)

    val bam = new File(getClass.getResource("/tutorial_8017/papa.bam").getFile)
    val referenceFile = new File(
      getClass
        .getResource("/tutorial_8017/chr19_chr19_KI270866v1_alt.fasta")
        .getFile)

    val (testConfig, basePath) = makeTestConfig
  }
}
