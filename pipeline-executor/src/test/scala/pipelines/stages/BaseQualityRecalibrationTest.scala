package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import java.io.File
import scala.concurrent.ExecutionContext.Implicits.global

class BaseQualityRecalibrationTest
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("BQSR should execute") {
    new Fixture {

      Given("a bam file and a reference")
      val result = withTaskSystem(testConfig) { implicit ts =>
        val indexedFasta = fetchIndexedReference(referenceFile)
        val input =
          TrainBQSRInput(
            bam = CoordinateSortedBam(await(SharedFile(bam, "some.bam")),
                                      await(SharedFile(bai, "some.bam.bai"))),
            reference = indexedFasta,
            knownSites = Set(
              VCF(await(SharedFile(vcf, "some.vcf")),
                  Some(await(SharedFile(vcfIdx, "some.vcf.idx")))))
          )

        When("executing the base quality recalibration steps")
        val future =
          for {
            table <- BaseQualityScoreRecalibration.trainBQSR(input)(
              ResourceRequest(1, 3000))
            recalibratedBam <- BaseQualityScoreRecalibration.applyBQSR(
              ApplyBQSRInput(input.bam, input.reference, table))(
              ResourceRequest(1, 3000))
            filePath <- recalibratedBam.bam.file
          } yield filePath

        await(future)
      }

      Then("a recalibrated bam file should be generated")
      result.get.canRead shouldBe true
      And("the stdout should be saved")
      new File(basePath.getAbsolutePath + "/some.bqsr.apply.stdout").canRead shouldBe true

    }
  }

  trait Fixture {

    val bam = new File(
      getClass.getResource("/tutorial_8017/papa.bam.sorted").getFile)
    val bai = new File(
      getClass.getResource("/tutorial_8017/papa.bam.sorted.bai").getFile)
    val referenceFile = new File(
      getClass
        .getResource("/tutorial_8017/chr19_chr19_KI270866v1_alt.fasta")
        .getFile)

    val vcf = new File(
      getClass
        .getResource("/example.vcf")
        .getFile)

    val vcfIdx = new File(
      getClass
        .getResource("/example.vcf.idx")
        .getFile)

    val (testConfig, basePath) = makeTestConfig
  }
}
