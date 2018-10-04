package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import java.io.File
import scala.concurrent.ExecutionContext.Implicits.global
import org.gc.pipelines.model._

class AlignmentQCTest
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("Parse AlignmentSummaryMetrics") {
    new Fixture {
      Given("an output table from picard's AlignmentSummaryMetrics")
      When("we try to parse it")
      AlignmentSummaryMetrics
        .Root(alignmentSummaryMetricsText, project, sampleId, runId)
      Then("it should not fail")
    }
  }

  test("AlignmentQC general should produce expected files") {
    new Fixture {

      Given("a bam file and a reference")
      val result = withTaskSystem(testConfig) { implicit ts =>
        val indexedFasta = fetchIndexedReference(referenceFile)
        val input =
          AlignmentQCInput(
            bam = CoordinateSortedBam(await(SharedFile(bam, "some.bam")),
                                      await(SharedFile(bai, "some.bam.bai"))),
            reference = indexedFasta
          )

        When("executing the general alignment qc step")
        val future =
          for {
            qcMetrics <- AlignmentQC.general(input)(CPUMemoryRequest(1, 3000))
          } yield qcMetrics

        await(future.flatMap(_.alignmentSummary.file))
      }

      Then("at least the alignment summary metrics file should be generated")
      result.get.canRead shouldBe true

    }
  }

  trait Fixture {

    val bam = new File(getClass.getResource("/tutorial_8017/papa.bam").getFile)
    val bai = new File(
      getClass.getResource("/tutorial_8017/papa.bam.bai").getFile)
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

    val alignmentSummaryMetricsText =
      """## htsjdk.samtools.metrics.StringHeader
# CollectMultipleMetrics  --INPUT /private/var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/tasks2018_10_04_10_38_16/tasks2018_10_04_10_38_166458571071156798966.temp/some.bam --ASSUME_SORTED true --OUTPUT /var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/fileutil2018_10_04_10_38_190/fileutil2018_10_04_10_38_199199210314413968356.qc --METRIC_ACCUMULATION_LEVEL READ_GROUP --METRIC_ACCUMULATION_LEVEL ALL_READS --PROGRAM CollectAlignmentSummaryMetrics --PROGRAM CollectSequencingArtifactMetrics --REFERENCE_SEQUENCE /private/var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/tasks2018_10_04_10_38_16/tasks2018_10_04_10_38_166458571071156798966.temp/referenceFasta.fasta  --STOP_AFTER 0 --INCLUDE_UNPAIRED false --VERBOSITY INFO --QUIET false --VALIDATION_STRINGENCY STRICT --COMPRESSION_LEVEL 5 --MAX_RECORDS_IN_RAM 500000 --CREATE_INDEX false --CREATE_MD5_FILE false --GA4GH_CLIENT_SECRETS client_secrets.json --help false --version false --showHidden false --USE_JDK_DEFLATER false --USE_JDK_INFLATER false
## htsjdk.samtools.metrics.StringHeader
# Started on: Thu Oct 04 10:38:20 CEST 2018

## METRICS CLASS	picard.analysis.AlignmentSummaryMetrics
CATEGORY	TOTAL_READS	PF_READS	PCT_PF_READS	PF_NOISE_READS	PF_READS_ALIGNED	PCT_PF_READS_ALIGNED	PF_ALIGNED_BASES	PF_HQ_ALIGNED_READS	PF_HQ_ALIGNED_BASES	PF_HQ_ALIGNED_Q20_BASES	PF_HQ_MEDIAN_MISMATCHES	PF_MISMATCH_RATE	PF_HQ_ERROR_RATE	PF_INDEL_RATE	MEAN_READ_LENGTH	READS_ALIGNED_IN_PAIRS	PCT_READS_ALIGNED_IN_PAIRS	PF_READS_IMPROPER_PAIRS	PCT_PF_READS_IMPROPER_PAIRS	BAD_CYCLES	STRAND_BALANCE	PCT_CHIMERAS	PCT_ADAPTER	SAMPLE	LIBRARY	READ_GROUP
FIRST_OF_PAIR	5000	5000	1	0	5000	1	755000	1028	155228	0	0	0.000082	0.000193	0	151	5000	1	0	0	0	0.4948	0	0			
SECOND_OF_PAIR	5000	5000	1	0	5000	1	755000	1028	155228	0	0	0.000081	0.000206	0	151	5000	1	0	0	0	0.5052	0	0			
PAIR	10000	10000	1	0	10000	1	1510000	2056	310456	0	0	0.000081	0.0002	0	151	10000	1	0	0	0	0.5	0	0			
FIRST_OF_PAIR	5000	5000	1	0	5000	1	755000	1028	155228	0	0	0.000082	0.000193	0	151	5000	1	0	0	0	0.4948	0	0	someProject.someSampleId	someProject.someSampleId	someRunId.L001
SECOND_OF_PAIR	5000	5000	1	0	5000	1	755000	1028	155228	0	0	0.000081	0.000206	0	151	5000	1	0	0	0	0.5052	0	0	someProject.someSampleId	someProject.someSampleId	someRunId.L001
PAIR	10000	10000	1	0	10000	1	1510000	2056	310456	0	0	0.000081	0.0002	0	151	10000	1	0	0	0	0.5	0	0	someProject.someSampleId	someProject.someSampleId	someRunId.L001


"""

    val project = Project("someProject")
    val sampleId = SampleId("someSampleId")
    val runId = RunId("someRunId")
    val lane = Lane("L001")
  }
}
