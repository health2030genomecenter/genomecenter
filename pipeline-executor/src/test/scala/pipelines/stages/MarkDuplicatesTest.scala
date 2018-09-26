package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import java.io.File

import org.gc.pipelines.model._

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
            bams = Set(Bam(await(SharedFile(bam, "some.bam")))),
            project = project,
            sampleId = sampleId,
            runId = runId
          )

        val future =
          BWAAlignment.mergeAndMarkDuplicate(input)(CPUMemoryRequest(1, 500))
        val bamWithSampleMetadata = await(future)
        val bamFile = await(bamWithSampleMetadata.bam.bam.file)
        await(bamWithSampleMetadata.bam.bam.history).context.get.dependencies.size shouldBe 1
        (bamWithSampleMetadata, bamFile)
      }

      val (bamWithMetadata, localBam) = result.get
      bamWithMetadata.project shouldBe project
      bamWithMetadata.runId shouldBe runId
      localBam.canRead shouldBe true
      new File(localBam.getParentFile, localBam.getName + ".stderr").canRead shouldBe true

      recordsInBamFile(localBam) shouldBe 10000

    }
  }

  trait Fixture {

    val project = Project("someProject")
    val sampleId = SampleId("someSampleId")
    val runId = RunId("someRunId")
    val lane = Lane("L001")

    val bam = new File(getClass.getResource("/tutorial_8017/papa.bam").getFile)
    val referenceFile = new File(
      getClass
        .getResource("/tutorial_8017/chr19_chr19_KI270866v1_alt.fasta")
        .getFile)

    val (testConfig, basePath) = makeTestConfig
  }
}
