package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import java.io.File
import scala.concurrent.ExecutionContext.Implicits.global

class HaplotypeCallerTest
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  ignore("genotypegvcf") {
    new Fixture {

      Given("a gvcf file and a reference")
      val result = withTaskSystem(testConfig) { implicit ts =>
        val indexedFasta = fetchIndexedReference(referenceFile)
        val dbsnpvcf = VCF(await(SharedFile(vcf, "dbsnp.vcf.gz")),
                           Some(await(SharedFile(vcfIdx, "dbsnp.vcf.gz.tbi"))))

        val input = GenotypeGVCFsInput(
          Set(
            VCF(await(SharedFile(gvcf, "some.vcf.gz")),
                Some(await(SharedFile(gvcfIndex, "some.vcf.gz.tbi"))))),
          indexedFasta,
          dbsnpvcf,
          "boo",
          dbsnpvcf,
          dbsnpvcf,
          dbsnpvcf,
          dbsnpvcf
        )

        When("genotyping them")
        val future =
          for {
            sites <- HaplotypeCaller.genotypeGvcfs(input)(
              ResourceRequest(1, 3000))
            sites <- sites.sites.vcf.file
          } yield sites

        await(future)
      }

      Then("a vcf file should be generated")
      val sites = result.get
      sites.canRead shouldBe true

    }
  }

  test("HaplotypeCaller should execute") {
    new Fixture {

      Given("a bam file and a reference")
      val result = withTaskSystem(testConfig) { implicit ts =>
        val indexedFasta = fetchIndexedReference(referenceFile)
        val input =
          HaplotypeCallerInput(
            CoordinateSortedBam(await(SharedFile(bam, "some.bam")),
                                await(SharedFile(bai, "some.bam.bai"))),
            indexedFasta
          )

        When("calling variants with haplotypecaller")
        val future =
          for {
            vcf <- HaplotypeCaller.haplotypeCaller(input)(
              ResourceRequest(1, 3000))
            filePath <- vcf.vcf.file
          } yield filePath

        await(future)
      }

      Then("a vcf file should be generated")
      result.get.canRead shouldBe true
      And("the stdout should be saved")
      new File(basePath.getAbsolutePath + "/some.bam.hc.gvcf.vcf.gz.stdout").canRead shouldBe true

    }
  }

  test("Collect variant calling metrics should execute") {
    new Fixture {

      Given("a bam file and a reference")
      val result = withTaskSystem(testConfig) { implicit ts =>
        val indexedFasta = fetchIndexedReference(referenceFile)
        val input = CollectVariantCallingMetricsInput(
          indexedFasta,
          VCF(await(SharedFile(gvcf, "some.vcf.gz")),
              Some(await(SharedFile(gvcfIndex, "some.vcf.gz.tbi")))),
          VCF(await(SharedFile(vcf, "dbsnp.vcf.gz")),
              Some(await(SharedFile(vcfIdx, "dbsnp.vcf.gz.tbi")))),
          BedFile(await(SharedFile(bed, "intervals")))
        )

        When("collect variant qc metrics")
        val future =
          for {
            metrics <- HaplotypeCaller.collectVariantCallingMetrics(input)(
              ResourceRequest(1, 3000))
            details <- metrics.details.file
            summary <- metrics.summary.file
          } yield (details, summary)

        await(future)
      }

      Then("two metric files should be generated")
      val (detail, summary) = result.get
      detail.canRead shouldBe true
      summary.canRead shouldBe true

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

    val bed = new File(
      getClass.getResource("/tutorial_8017/capture.bed").getFile)

    val gvcf = new File(
      getClass.getResource("/tutorial_8017/some.bam.hc.gvcf.vcf.gz").getFile)
    val gvcfIndex = new File(
      getClass
        .getResource("/tutorial_8017/some.bam.hc.gvcf.vcf.gz.tbi")
        .getFile)

    val vcf = new File(
      getClass
        .getResource("/example.vcf.gz")
        .getFile)

    val vcfIdx = new File(
      getClass
        .getResource("/example.vcf.gz.tbi")
        .getFile)

    val (testConfig, basePath) = makeTestConfig
  }
}
