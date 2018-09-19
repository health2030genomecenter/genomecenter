package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import java.io.File

import org.gc.pipelines.model._
import org.gc.pipelines.util.Exec

class BwaAlignmentTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("Bwa alignment stage should produce a clean bam file") {
    new Fixture {

      val result = withTaskSystem(testConfig) { implicit ts =>
        val input =
          PerLaneBWAAlignmentInput(
            read1 = FastQ(await(SharedFile(fastq1, "fastq1.gz"))),
            read2 = FastQ(await(SharedFile(fastq2, "fastq2.gz"))),
            project = project,
            sampleId = sampleId,
            runId = runId,
            lane = lane,
            reference = ReferenceFasta(
              await(SharedFile(referenceFile, "referenceFasta.fasta.gz")))
          )

        val future =
          BWAAlignment.alignSingleLane(input)(CPUMemoryRequest(1, 500))
        val bamWithSampleMetadata = await(future)
        val bamFile = await(bamWithSampleMetadata.bam.file.file)
        (bamWithSampleMetadata, bamFile)
      }

      val (bamWithMetadata, localBam) = result.get
      println(localBam)
      bamWithMetadata.project shouldBe project
      bamWithMetadata.runId shouldBe runId
      bamWithMetadata.bam.file.history.get.dependencies.size shouldBe 3
      localBam.canRead shouldBe true
      new File(localBam.getParentFile, localBam.getName.dropRight(3) + "stderr").canRead shouldBe true

      // TODO: replace this with htsjdk
      val (stdout, _, _) =
        Exec.bash("test")(s"samtools view ${localBam.getAbsolutePath} | wc -l")
      stdout.mkString.trim.toInt shouldBe 10000

    }
  }

  trait Fixture {

    val project = Project("someProject")
    val sampleId = SampleId("someSampleId")
    val runId = RunId("someRunId")
    val lane = Lane("L001")

    val fastq1 = new File(
      getClass.getResource("/tutorial_8017/papa.read1.fq.gz").getFile)
    val fastq2 = new File(
      getClass.getResource("/tutorial_8017/papa.read2.fq.gz").getFile)
    val referenceFile = new File(
      getClass
        .getResource("/tutorial_8017/chr19_chr19_KI270866v1_alt.fasta.gz")
        .getFile)

    val (testConfig, basePath) = makeTestConfig
  }
}
