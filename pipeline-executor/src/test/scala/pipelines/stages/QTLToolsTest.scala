package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import java.io.File
import scala.concurrent.ExecutionContext.Implicits.global

class QTLToolsTest
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {
  test("QTLTools should produce expected files") {
    new Fixture {

      Given("a bam file and a reference")
      val result = withTaskSystem(testConfig) { implicit ts =>
        val input =
          QTLToolsQuantificationInput(
            bam = CoordinateSortedBam(await(SharedFile(bam, "some.bam")),
                                      await(SharedFile(bai, "some.bam.bai"))),
            gtf = GTFFile(await(SharedFile(gtfFile, "gtf"))),
            additionalCommandLineArguments =
              List("--filter-mapping-quality", "255")
          )

        When("executing the general alignment qc step")
        val future =
          for {
            qcMetrics <- QTLToolsQuantification.quantify(input)(
              ResourceRequest(1, 3000))
          } yield qcMetrics

        await(future.flatMap(_.exonCounts.file))
      }

      Then(
        "at least the hybridication selection metrics file should be generated")
      result.get.canRead shouldBe true
      val stdout =
        new File(result.get.getParentFile, "some.bam.qtltools.quant.stdout")
      fileutils
        .openSource(stdout)(_.mkString)
        .contains("Minimum mapping quality: 255") shouldBe true

    }
  }

  trait Fixture {

    val bed = new File(
      getClass.getResource("/tutorial_8017/capture.bed").getFile)
    val bam = new File(
      getClass.getResource("/tutorial_8017/papa.bam.sorted").getFile)
    val bai = new File(
      getClass.getResource("/tutorial_8017/papa.bam.sorted.bai").getFile)
    val referenceFile = new File(
      getClass
        .getResource("/tutorial_8017/chr19_chr19_KI270866v1_alt.fasta")
        .getFile)

    val (testConfig, basePath) = makeTestConfig

    val gtfFile = new File(
      getClass
        .getResource("/short.gtf")
        .getFile)

  }
}
