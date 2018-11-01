package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import java.io.File

import org.gc.pipelines.model._

class StarAlignmentTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("STAR index creation") {
    new Fixture {

      withTaskSystem(testConfig) { implicit ts =>
        val fasta = fetchReference(referenceFile)

        val result =
          await(StarAlignment.indexReference(fasta)(ResourceRequest(1, 500)))

        result.indexFiles.size shouldBe 8
        import scala.concurrent.ExecutionContext.Implicits.global
        new File(await(result.genomeFolder), "SAIndex").delete
        new File(await(result.genomeFolder), "SA").delete
        new File(await(result.genomeFolder), "Genome").delete

      }

    }
  }

  test("STAR alignment") {
    new Fixture {

      withTaskSystem(testConfig) { implicit ts =>
        val indexedFasta =
          fetchStarIndexedReference(referenceFile, genomeFolder)

        val input =
          PerLaneStarAlignmentInput(
            read1 = FastQ(await(SharedFile(fastq1, "fastq1.gz")), 10000L),
            read2 = FastQ(await(SharedFile(fastq2, "fastq2.gz")), 10000L),
            project = project,
            sampleId = sampleId,
            runId = runId,
            lane = lane,
            partition = PartitionId(0),
            reference = indexedFasta,
            gtf = await(SharedFile(gtfFile, "gtf")),
            readLength = 151
          )

        val result =
          await(StarAlignment.alignSingleLane(input)(ResourceRequest(1, 500)))

        val bamFile = await(result.bam.bam.file.file)
        val finalLog = await(result.finalLog.file)
        recordsInBamFile(bamFile) shouldBe 17876
        StarMetrics.Root(fileutils.openSource(finalLog)(_.mkString),
                         project,
                         sampleId,
                         runId,
                         lane)

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
    val gtfFile = new File(
      getClass
        .getResource("/short.gtf")
        .getFile)

    val genomeFolder = new File(
      getClass
        .getResource("/tutorial_8017/chr19_chr19_KI270866v1_alt.fasta.star/")
        .getFile)

    val (testConfig, basePath) = makeTestConfig
  }
}
