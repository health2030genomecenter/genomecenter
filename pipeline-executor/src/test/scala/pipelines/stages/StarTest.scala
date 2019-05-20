package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import java.io.File

import org.gc.pipelines.model._
import org.gc.pipelines.util.StableSet

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
          await(
            StarAlignment.indexReference(
              StarIndexInput(fasta, StarVersion.Star261a))(
              ResourceRequest(1, 500)))

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
          StarAlignmentInput(
            fastqs = StableSet(
              FastQPerLane(
                runId,
                Lane(1),
                FastQ(await(SharedFile(fastq1, "fastq1.gz")), 10000L, Some(75)),
                FastQ(await(SharedFile(fastq2, "fastq2.gz")), 10000L, Some(75)),
                None,
                PartitionId(0)
              )),
            project = project,
            sampleId = sampleId,
            reference = indexedFasta,
            gtf = await(SharedFile(gtfFile, "gtf")),
            readLength = 151,
            starVersion = StarVersion.Star261a
          )

        val result =
          await(StarAlignment.alignSample(input)(ResourceRequest(1, 500)))

        val bamFile = await(result.bam.bam.file.file)
        val finalLog = await(result.finalLog.file)
        recordsInBamFile(bamFile) shouldBe 10000
        println(fileutils.openSource(finalLog)(_.mkString))
        val metrics =
          StarMetrics.Root(fileutils.openSource(finalLog)(_.mkString),
                           project,
                           sampleId)
        println(metrics.metrics)
        metrics.metrics.meanReadLength shouldBe 302d
        metrics.metrics.numberOfReads shouldBe 5000

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
