package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import java.io.File

import org.gc.pipelines.model._

class BwaAlignmentTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("Bwa alignment stage should produce a clean bam file") {
    new Fixture {

      val result = withTaskSystem(testConfig) { implicit ts =>
        val indexedFasta = fetchIndexedReference(referenceFile)

        val input =
          PerLaneBWAAlignmentInput(
            read1 = FastQ(await(SharedFile(fastq1, "fastq1.gz"))),
            read2 = FastQ(await(SharedFile(fastq2, "fastq2.gz"))),
            project = project,
            sampleId = sampleId,
            runId = runId,
            lane = lane,
            reference = indexedFasta,
            umi = None
          )

        val future =
          BWAAlignment.alignSingleLane(input)(CPUMemoryRequest(1, 500))
        val bamWithSampleMetadata = await(future)
        val bamFile = await(bamWithSampleMetadata.bam.file.file)
        await(bamWithSampleMetadata.bam.file.history).context.get.dependencies.size shouldBe 3
        (bamWithSampleMetadata, bamFile)
      }

      val (bamWithMetadata, localBam) = result.get
      bamWithMetadata.project shouldBe project
      bamWithMetadata.runId shouldBe runId
      localBam.canRead shouldBe true
      new File(localBam.getParentFile, localBam.getName + ".stderr").canRead shouldBe true

      recordsInBamFile(localBam) shouldBe 10000

      getSortOrder(localBam) shouldBe "queryname"

      takeRecordsInBamFile(localBam, 100).foreach { record =>
        record.getReadUnmappedFlag shouldBe false
        record.getReferenceName.take(5) shouldBe "chr19"
      }

    }
  }

  test(
    "Bwa alignment stage should produce a clean bam file and process UMIs if present") {
    new Fixture {

      val result = withTaskSystem(testConfig) { implicit ts =>
        val indexedFasta = fetchIndexedReference(referenceFile)

        val input =
          PerLaneBWAAlignmentInput(
            read1 = FastQ(await(SharedFile(fastq1, "fastq1.gz"))),
            read2 = FastQ(await(SharedFile(fastq2, "fastq2.gz"))),
            project = project,
            sampleId = sampleId,
            runId = runId,
            lane = lane,
            reference = indexedFasta,
            umi = Some(FastQ(await(SharedFile(fastq2, "fastq2.gz"))))
          )

        val future =
          BWAAlignment.alignSingleLane(input)(CPUMemoryRequest(1, 500))
        val bamWithSampleMetadata = await(future)
        val bamFile = await(bamWithSampleMetadata.bam.file.file)
        await(bamWithSampleMetadata.bam.file.history).context.get.dependencies.size shouldBe 4
        (bamWithSampleMetadata, bamFile)
      }

      val (bamWithMetadata, localBam) = result.get
      bamWithMetadata.project shouldBe project
      bamWithMetadata.runId shouldBe runId
      localBam.canRead shouldBe true
      new File(localBam.getParentFile, localBam.getName + ".stderr").canRead shouldBe true

      recordsInBamFile(localBam) shouldBe 10000

      getSortOrder(localBam) shouldBe "queryname"

      takeRecordsInBamFile(localBam, 100).foreach { record =>
        record.getReadUnmappedFlag shouldBe false
        record.getReferenceName.take(5) shouldBe "chr19"
        record.getAttribute("OX").toString.size shouldBe 151
      }

    }
  }

  trait Fixture {

    val project = Project("someProject")
    val sampleId = SampleId("someSampleId")
    val runId = RunId("someRunId")
    val lane = Lane(1)

    val fastq1 = new File(
      getClass.getResource("/tutorial_8017/papa.read1.fq.gz").getFile)
    val fastq2 = new File(
      getClass.getResource("/tutorial_8017/papa.read2.fq.gz").getFile)
    val referenceFile = new File(
      getClass
        .getResource("/tutorial_8017/chr19_chr19_KI270866v1_alt.fasta")
        .getFile)

    val (testConfig, basePath) = makeTestConfig
  }
}
