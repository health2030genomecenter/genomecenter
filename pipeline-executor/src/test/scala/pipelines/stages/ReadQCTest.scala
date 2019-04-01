package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import java.io.File
import org.gc.pipelines.util.StableSet
import org.gc.pipelines.model._

class ReadQCTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("fastp should generate an html and json report about a fastq") {
    new Fixture {

      val result = withTaskSystem(testConfig) { implicit ts =>
        val input =
          PerSampleFastQ(
            StableSet(
              FastQPerLane(
                lane = lane,
                read1 =
                  FastQ(await(SharedFile(fastq1, "fastq1.gz")), 10000L, None),
                read2 =
                  FastQ(await(SharedFile(fastq2, "fastq2.gz")), 10000L, None),
                umi = None,
                runId = runId,
                partition = PartitionId(0)
              )),
            project = project,
            sampleId = sampleId
          )

        val future =
          ReadQC.readQC(ReadQCInput(StableSet(input), runId))(
            ResourceRequest(1, 500))
        val report = await(future)

        val plot = await(report.plots.file)
        plot.canRead shouldBe true
        plot
      }

      result.get

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
