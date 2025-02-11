package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import java.io.File
import scala.concurrent.ExecutionContext.Implicits.global
import org.gc.pipelines.model._
import org.gc.pipelines.model.{FastpReport => FastpReportModel}

class AlignmentQCTest
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("Render html table") {
    new Fixture {
      val dup = DuplicationMetrics
        .Root(duplicationMetricsText, project, sampleId)
      val insertSize = InsertSizeMetrics
        .Root(insertSizeMetricsText, project, sampleId)
      val als = AlignmentSummaryMetrics
        .Root(alignmentSummaryMetricsText, project, sampleId)
      val hs = HsMetrics
        .Root(hsMetricsFile, project, sampleId)
      val wgs = WgsMetrics
        .Root(wgsMetricsFile, project, sampleId)
      val fastp = FastpReportModel
        .Root(fastpText, project, sampleId, runId)
      val vcfQc =
        VariantCallingMetrics.Root(variantCallingMetrics, project, sampleId)
      val joined = als.map { alSummaryOfLane =>
        val lane = alSummaryOfLane.lane
        val hsMetricsOfLane = hs.find(_.lane == lane).get
        (alSummaryOfLane, hsMetricsOfLane, AnalysisId("boo"))
      }
      println(
        AlignmentQC.makeHtmlTable(
          joined,
          List(
            (dup,
             fastp,
             wgs,
             Some(vcfQc),
             Some(vcfQc),
             insertSize,
             AnalysisId("boo"),
             List(RunId("someRunId") -> MeanCoverageResult(1.0, 3.0)))),
          List()
        ))
    }
  }

  test("Parse DuplicationMetrics") {
    new Fixture {
      Given("an output table from picard's DuplicationMetrics")
      When("we try to parse it")
      DuplicationMetrics
        .Root(duplicationMetricsText, project, sampleId)
      DuplicationMetrics
        .Root(duplicationMetricsText2, Project("project1"), SampleId("GIB"))
      Then("it should not fail")
    }
  }

  test("Parse AlignmentSummaryMetrics") {
    new Fixture {
      Given("an output table from picard's AlignmentSummaryMetrics")
      When("we try to parse it")
      AlignmentSummaryMetrics
        .Root(alignmentSummaryMetricsText, project, sampleId)
      Then("it should not fail")
    }
  }

  test("Parse HsMetrics") {
    new Fixture {
      Given("an output table from picard's HsMetrics")
      When("we try to parse it")
      HsMetrics
        .Root(hsMetricsFile, project, sampleId)
      Then("it should not fail")
    }
  }

  test("SelectionQC  should produce expected files") {
    new Fixture {

      Given("a bam file and a reference")
      val result = withTaskSystem(testConfig) { implicit ts =>
        val indexedFasta = fetchIndexedReference(referenceFile)
        val input =
          SelectionQCInput(
            bam = CoordinateSortedBam(await(SharedFile(bam, "some.bam")),
                                      await(SharedFile(bai, "some.bam.bai"))),
            reference = indexedFasta,
            selectionTargetIntervals =
              BedFile(await(SharedFile(bed, "some.bed")))
          )

        When("executing the general alignment qc step")
        val future =
          for {
            qcMetrics <- AlignmentQC.hybridizationSelection(input)(
              ResourceRequest(1, 3000))
          } yield qcMetrics

        await(future.flatMap(_.hsMetrics.file))
      }

      Then(
        "at least the hybridication selection metrics file should be generated")
      result.get.canRead shouldBe true

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
            qcMetrics <- AlignmentQC.general(input)(ResourceRequest(1, 3000))
          } yield qcMetrics

        await(future.flatMap(_.alignmentSummary.file))
      }

      Then("at least the alignment summary metrics file should be generated")
      result.get.canRead shouldBe true

    }
  }

  test("CollectWGSMetrics should produce expected files") {
    new Fixture {

      Given("a bam file and a reference")
      val result = withTaskSystem(testConfig) { implicit ts =>
        val indexedFasta = fetchIndexedReference(referenceFile)
        val input =
          CollectWholeGenomeMetricsInput(
            bam = CoordinateSortedBam(await(SharedFile(bam, "some.bam")),
                                      await(SharedFile(bai, "some.bam.bai"))),
            reference = indexedFasta
          )

        When("executing the wgs alignment qc step")
        val future =
          for {
            qcMetrics <- AlignmentQC.wholeGenomeMetrics(input)(
              ResourceRequest(1, 3000))
          } yield qcMetrics

        await(future.flatMap(_.wgsMetrics.file))
      }

      Then("at least the alignment summary metrics file should be generated")
      result.get.canRead shouldBe true
      println(result)
      fileutils.openSource(result.get) { s =>
        val data = s.mkString
        WgsMetrics
          .Root(data, project, sampleId)
          .metrics
          .genomeTerritory shouldBe 55809553L
      }

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
FIRST_OF_PAIR	5000	5000	1	0	5000	1	755000	1028	155228	0	0	0.000082	0.000193	0	151	5000	1	0	0	0	0.4948	0	0	someProject.someSampleId	someProject.someSampleId	someRunId.1
SECOND_OF_PAIR	5000	5000	1	0	5000	1	755000	1028	155228	0	0	0.000081	0.000206	0	151	5000	1	0	0	0	0.5052	0	0	someProject.someSampleId	someProject.someSampleId	someRunId.1
PAIR	10000	10000	1	0	10000	1	1510000	2056	310456	0	0	0.000081	0.0002	0	151	10000	1	0	0	0	0.5	0	0	someProject.someSampleId	someProject.someSampleId	someRunId.1


"""

    val hsMetricsFile =
      """## htsjdk.samtools.metrics.StringHeader
# CollectHsMetrics  --BAIT_INTERVALS /var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/fileutil2018_10_04_14_42_150/fileutil2018_10_04_14_42_154101170785414879512 --BAIT_SET_NAME some.bed --TARGET_INTERVALS /var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/fileutil2018_10_04_14_42_150/fileutil2018_10_04_14_42_154101170785414879512 --INPUT /private/var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/tasks2018_10_04_14_42_11/tasks2018_10_04_14_42_118573417766713096981.temp/some.bam --OUTPUT /var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/fileutil2018_10_04_14_42_150/fileutil2018_10_04_14_42_151781444983663316224.qc --METRIC_ACCUMULATION_LEVEL READ_GROUP --METRIC_ACCUMULATION_LEVEL ALL_READS --REFERENCE_SEQUENCE /private/var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/tasks2018_10_04_14_42_11/tasks2018_10_04_14_42_118573417766713096981.temp/referenceFasta.fasta  --NEAR_DISTANCE 250 --MINIMUM_MAPPING_QUALITY 20 --MINIMUM_BASE_QUALITY 20 --CLIP_OVERLAPPING_READS true --COVERAGE_CAP 200 --SAMPLE_SIZE 10000 --ALLELE_FRACTION 0.001 --ALLELE_FRACTION 0.005 --ALLELE_FRACTION 0.01 --ALLELE_FRACTION 0.02 --ALLELE_FRACTION 0.05 --ALLELE_FRACTION 0.1 --ALLELE_FRACTION 0.2 --ALLELE_FRACTION 0.3 --ALLELE_FRACTION 0.5 --VERBOSITY INFO --QUIET false --VALIDATION_STRINGENCY STRICT --COMPRESSION_LEVEL 5 --MAX_RECORDS_IN_RAM 500000 --CREATE_INDEX false --CREATE_MD5_FILE false --GA4GH_CLIENT_SECRETS client_secrets.json --help false --version false --showHidden false --USE_JDK_DEFLATER false --USE_JDK_INFLATER false
## htsjdk.samtools.metrics.StringHeader
# Started on: Thu Oct 04 14:42:17 CEST 2018

## METRICS CLASS	picard.analysis.directed.HsMetrics
BAIT_SET	GENOME_SIZE	BAIT_TERRITORY	TARGET_TERRITORY	BAIT_DESIGN_EFFICIENCY	TOTAL_READS	PF_READS	PF_UNIQUE_READS	PCT_PF_READS	PCT_PF_UQ_READS	PF_UQ_READS_ALIGNED	PCT_PF_UQ_READS_ALIGNED	PF_BASES_ALIGNED	PF_UQ_BASES_ALIGNED	ON_BAIT_BASES	NEAR_BAIT_BASES	OFF_BAIT_BASES	ON_TARGET_BASES	PCT_SELECTED_BASES	PCT_OFF_BAIT	ON_BAIT_VS_SELECTED	MEAN_BAIT_COVERAGE	MEAN_TARGET_COVERAGE	MEDIAN_TARGET_COVERAGE	MAX_TARGET_COVERAGE	PCT_USABLE_BASES_ON_BAIT	PCT_USABLE_BASES_ON_TARGET	FOLD_ENRICHMENT	ZERO_CVG_TARGETS_PCT	PCT_EXC_DUPE	PCT_EXC_MAPQ	PCT_EXC_BASEQ	PCT_EXC_OVERLAP	PCT_EXC_OFF_TARGET	FOLD_80_BASE_PENALTY	PCT_TARGET_BASES_1X	PCT_TARGET_BASES_2X	PCT_TARGET_BASES_10X	PCT_TARGET_BASES_20X	PCT_TARGET_BASES_30X	PCT_TARGET_BASES_40X	PCT_TARGET_BASES_50X	PCT_TARGET_BASES_100X	HS_LIBRARY_SIZE	HS_PENALTY_10X	HS_PENALTY_20X	HS_PENALTY_30X	HS_PENALTY_40X	HS_PENALTY_50X	HS_PENALTY_100X	AT_DROPOUT	GC_DROPOUT	HET_SNP_SENSITIVITY	HET_SNP_Q	SAMPLE	LIBRARY	READ_GROUP
some.bed	58660772	58617616	58617616	1	10000	10000	10000	1	1	10000	1	1510000	1510000	931066	0	578934	0	0.6166	0.3834	1	0.015884	0	0	0	0.6166	0	0.617054	1	0	0.7944	0.2056	0	0.2056	?	0	0	0	0	0	0	0	0		0	0	0	0	0	0	0	0	0.000099	0			
some.bed	58660772	58617616	58617616	1	10000	10000	10000	1	1	10000	1	1510000	1510000	931066	0	578934	0	0.6166	0.3834	1	0.015884	0	0	0	0.6166	0	0.617054	1	0	0.7944	0.2056	0	0.2056	?	0	0	0	0	0	0	0	0		0	0	0	0	0	0	0	0	0.000099	0	someProject.someSampleId	someProject.someSampleId	someRunId.1

## HISTOGRAM	java.lang.Integer
coverage_or_base_quality	high_quality_coverage_count	unfiltered_baseq_count	high_quality_coverage_count	unfiltered_baseq_count
0	117235232	0	117235232	0
1	0	0	0	0
2	0	0	0	0
3	0	0	0	0
4	0	0	0	0
5	0	0	0	0
6	0	0	0	0
7	0	0	0	0
8	0	0	0	0
9	0	620912	0	620912
10	0	0	0	0
11	0	0	0	0
12	0	0	0	0
13	0	0	0	0
14	0	0	0	0
15	0	0	0	0
16	0	0	0	0
17	0	0	0	0
18	0	0	0	0
19	0	0	0	0
20	0	0	0	0
21	0	0	0	0
22	0	0	0	0
23	0	0	0	0
24	0	0	0	0
25	0	0	0	0
26	0	0	0	0
27	0	0	0	0
28	0	0	0	0
29	0	0	0	0
30	0	0	0	0
31	0	0	0	0
32	0	0	0	0
33	0	0	0	0
34	0	0	0	0
35	0	0	0	0
36	0	0	0	0
37	0	0	0	0
38	0	0	0	0
39	0	0	0	0
40	0	0	0	0
41	0	0	0	0
42	0	0	0	0
43	0	0	0	0
44	0	0	0	0
45	0	0	0	0
46	0	0	0	0
47	0	0	0	0
48	0	0	0	0
49	0	0	0	0
50	0	0	0	0
51	0	0	0	0
52	0	0	0	0
53	0	0	0	0
54	0	0	0	0
55	0	0	0	0
56	0	0	0	0
57	0	0	0	0
58	0	0	0	0
59	0	0	0	0
60	0	0	0	0
61	0	0	0	0
62	0	0	0	0
63	0	0	0	0
64	0	0	0	0
65	0	0	0	0
66	0	0	0	0
67	0	0	0	0
68	0	0	0	0
69	0	0	0	0
70	0	0	0	0
71	0	0	0	0
72	0	0	0	0
73	0	0	0	0
74	0	0	0	0
75	0	0	0	0
76	0	0	0	0
77	0	0	0	0
78	0	0	0	0
79	0	0	0	0
80	0	0	0	0
81	0	0	0	0
82	0	0	0	0
83	0	0	0	0
84	0	0	0	0
85	0	0	0	0
86	0	0	0	0
87	0	0	0	0
88	0	0	0	0
89	0	0	0	0
90	0	0	0	0
91	0	0	0	0
92	0	0	0	0
93	0	0	0	0
94	0	0	0	0
95	0	0	0	0
96	0	0	0	0
97	0	0	0	0
98	0	0	0	0
99	0	0	0	0
100	0	0	0	0
101	0	0	0	0
102	0	0	0	0
103	0	0	0	0
104	0	0	0	0
105	0	0	0	0
106	0	0	0	0
107	0	0	0	0
108	0	0	0	0
109	0	0	0	0
110	0	0	0	0
111	0	0	0	0
112	0	0	0	0
113	0	0	0	0
114	0	0	0	0
115	0	0	0	0
116	0	0	0	0
117	0	0	0	0
118	0	0	0	0
119	0	0	0	0
120	0	0	0	0
121	0	0	0	0
122	0	0	0	0
123	0	0	0	0
124	0	0	0	0
125	0	0	0	0
126	0	0	0	0
127	0	0	0	0
128	0	0	0	0
129	0	0	0	0
130	0	0	0	0
131	0	0	0	0
132	0	0	0	0
133	0	0	0	0
134	0	0	0	0
135	0	0	0	0
136	0	0	0	0
137	0	0	0	0
138	0	0	0	0
139	0	0	0	0
140	0	0	0	0
141	0	0	0	0
142	0	0	0	0
143	0	0	0	0
144	0	0	0	0
145	0	0	0	0
146	0	0	0	0
147	0	0	0	0
148	0	0	0	0
149	0	0	0	0
150	0	0	0	0
151	0	0	0	0
152	0	0	0	0
153	0	0	0	0
154	0	0	0	0
155	0	0	0	0
156	0	0	0	0
157	0	0	0	0
158	0	0	0	0
159	0	0	0	0
160	0	0	0	0
161	0	0	0	0
162	0	0	0	0
163	0	0	0	0
164	0	0	0	0
165	0	0	0	0
166	0	0	0	0
167	0	0	0	0
168	0	0	0	0
169	0	0	0	0
170	0	0	0	0
171	0	0	0	0
172	0	0	0	0
173	0	0	0	0
174	0	0	0	0
175	0	0	0	0
176	0	0	0	0
177	0	0	0	0
178	0	0	0	0
179	0	0	0	0
180	0	0	0	0
181	0	0	0	0
182	0	0	0	0
183	0	0	0	0
184	0	0	0	0
185	0	0	0	0
186	0	0	0	0
187	0	0	0	0
188	0	0	0	0
189	0	0	0	0
190	0	0	0	0
191	0	0	0	0
192	0	0	0	0
193	0	0	0	0
194	0	0	0	0
195	0	0	0	0
196	0	0	0	0
197	0	0	0	0
198	0	0	0	0
199	0	0	0	0
200	0	0	0	0

"""

    val duplicationMetricsText =
      """## htsjdk.samtools.metrics.StringHeader
# MarkDuplicates  --INPUT /private/var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/tasks2018_10_04_16_05_34/tasks2018_10_04_16_05_342561162817543363859.temp/some.bam --OUTPUT /var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/fileutil2018_10_04_16_05_360/fileutil2018_10_04_16_05_36253803680595703226.bam --METRICS_FILE /var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/fileutil2018_10_04_16_05_360/fileutil2018_10_04_16_05_366905709434336158524.metrics --OPTICAL_DUPLICATE_PIXEL_DISTANCE 250 --TMP_DIR /var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/fileutil2018_10_04_16_05_360/fileutil2018_10_04_16_05_363105775668648271251.markDuplicateTempFolder --CREATE_INDEX true  --MAX_SEQUENCES_FOR_DISK_READ_ENDS_MAP 50000 --MAX_FILE_HANDLES_FOR_READ_ENDS_MAP 8000 --SORTING_COLLECTION_SIZE_RATIO 0.25 --TAG_DUPLICATE_SET_MEMBERS false --REMOVE_SEQUENCING_DUPLICATES false --TAGGING_POLICY DontTag --CLEAR_DT true --ADD_PG_TAG_TO_READS true --REMOVE_DUPLICATES false --ASSUME_SORTED false --DUPLICATE_SCORING_STRATEGY SUM_OF_BASE_QUALITIES --PROGRAM_RECORD_ID MarkDuplicates --PROGRAM_GROUP_NAME MarkDuplicates --READ_NAME_REGEX <optimized capture of last three ':' separated fields as numeric values> --MAX_OPTICAL_DUPLICATE_SET_SIZE 300000 --VERBOSITY INFO --QUIET false --VALIDATION_STRINGENCY STRICT --COMPRESSION_LEVEL 5 --MAX_RECORDS_IN_RAM 500000 --CREATE_MD5_FILE false --GA4GH_CLIENT_SECRETS client_secrets.json --help false --version false --showHidden false --USE_JDK_DEFLATER false --USE_JDK_INFLATER false
## htsjdk.samtools.metrics.StringHeader
# Started on: Thu Oct 04 16:05:39 CEST 2018

## METRICS CLASS	picard.sam.DuplicationMetrics
LIBRARY	UNPAIRED_READS_EXAMINED	READ_PAIRS_EXAMINED	SECONDARY_OR_SUPPLEMENTARY_RDS	UNMAPPED_READS	UNPAIRED_READ_DUPLICATES	READ_PAIR_DUPLICATES	READ_PAIR_OPTICAL_DUPLICATES	PERCENT_DUPLICATION	ESTIMATED_LIBRARY_SIZE
someProject.someSampleId	0	5000	0	0	0	0	0	0	


"""

    val duplicationMetricsText2 =
      """## htsjdk.samtools.metrics.StringHeader
# MarkDuplicates  --INPUT /tmp/tasks2018_10_05_08_11_46/tasks2018_10_05_08_11_467560874900561946991.temp/projects/project1/whateverRunId/intermediate/project1.GIB.whateverRunId.L001.bam --OUTPUT /tmp/fileutil2018_10_05_08_11_440/fileutil2018_10_05_08_11_443267388414674492666.bam --METRICS_FILE /tmp/fileutil2018_10_05_08_11_440/fileutil2018_10_05_08_11_445813249422001716734.metrics --OPTICAL_DUPLICATE_PIXEL_DISTANCE 250 --TMP_DIR /tmp/fileutil2018_10_05_08_11_440/fileutil2018_10_05_08_11_445060739684805306.markDuplicateTempFolder --CREATE_INDEX true  --MAX_SEQUENCES_FOR_DISK_READ_ENDS_MAP 50000 --MAX_FILE_HANDLES_FOR_READ_ENDS_MAP 8000 --SORTING_COLLECTION_SIZE_RATIO 0.25 --TAG_DUPLICATE_SET_MEMBERS false --REMOVE_SEQUENCING_DUPLICATES false --TAGGING_POLICY DontTag --CLEAR_DT true --ADD_PG_TAG_TO_READS true --REMOVE_DUPLICATES false --ASSUME_SORTED false --DUPLICATE_SCORING_STRATEGY SUM_OF_BASE_QUALITIES --PROGRAM_RECORD_ID MarkDuplicates --PROGRAM_GROUP_NAME MarkDuplicates --READ_NAME_REGEX <optimized capture of last three ':' separated fields as numeric values> --MAX_OPTICAL_DUPLICATE_SET_SIZE 300000 --VERBOSITY INFO --QUIET false --VALIDATION_STRINGENCY STRICT --COMPRESSION_LEVEL 5 --MAX_RECORDS_IN_RAM 500000 --CREATE_MD5_FILE false --GA4GH_CLIENT_SECRETS client_secrets.json --help false --version false --showHidden false --USE_JDK_DEFLATER false --USE_JDK_INFLATER false
## htsjdk.samtools.metrics.StringHeader
# Started on: Fri Oct 05 08:14:01 UTC 2018

## METRICS CLASS	picard.sam.DuplicationMetrics
LIBRARY	UNPAIRED_READS_EXAMINED	READ_PAIRS_EXAMINED	SECONDARY_OR_SUPPLEMENTARY_RDS	UNMAPPED_READS	UNPAIRED_READ_DUPLICATES	READ_PAIR_DUPLICATES	READ_PAIR_OPTICAL_DUPLICATES	PERCENT_DUPLICATION	ESTIMATED_LIBRARY_SIZE
project1.GIB	7273	6937	194	66865	36	8	1	0.002459	3433980

## HISTOGRAM	java.lang.Double
BIN	VALUE
1.0	1.000144
2.0	1.99827
3.0	2.994381
4.0	3.988482
5.0	4.980577
6.0	5.97067
7.0	6.958765
8.0	7.944866
9.0	8.928977
10.0	9.911101
11.0	10.891244
12.0	11.869409
13.0	12.8456
14.0	13.81982
15.0	14.792075
16.0	15.762368
17.0	16.730702
18.0	17.697083
19.0	18.661513
20.0	19.623997
21.0	20.584538
22.0	21.543141
23.0	22.49981
24.0	23.454548
25.0	24.407359
26.0	25.358247
27.0	26.307217
28.0	27.254271
29.0	28.199414
30.0	29.14265
31.0	30.083982
32.0	31.023415
33.0	31.960952
34.0	32.896596
35.0	33.830353
36.0	34.762225
37.0	35.692216
38.0	36.620331
39.0	37.546573
40.0	38.470946
41.0	39.393453
42.0	40.314098
43.0	41.232886
44.0	42.149819
45.0	43.064902
46.0	43.978138
47.0	44.889531
48.0	45.799085
49.0	46.706804
50.0	47.612691
51.0	48.516749
52.0	49.418983
53.0	50.319396
54.0	51.217992
55.0	52.114775
56.0	53.009748
57.0	53.902915
58.0	54.794279
59.0	55.683845
60.0	56.571615
61.0	57.457594
62.0	58.341785
63.0	59.224191
64.0	60.104817
65.0	60.983666
66.0	61.860741
67.0	62.736046
68.0	63.609584
69.0	64.48136
70.0	65.351376
71.0	66.219637
72.0	67.086145
73.0	67.950905
74.0	68.81392
75.0	69.675193
76.0	70.534728
77.0	71.392528
78.0	72.248597
79.0	73.102939
80.0	73.955556
81.0	74.806453
82.0	75.655633
83.0	76.503099
84.0	77.348854
85.0	78.192903
86.0	79.035249
87.0	79.875895
88.0	80.714844
89.0	81.5521
90.0	82.387667
91.0	83.221547
92.0	84.053744
93.0	84.884262
94.0	85.713104
95.0	86.540274
96.0	87.365774
97.0	88.189608
98.0	89.011779
99.0	89.832292
100.0	90.651148

"""

    val wgsMetricsFile =
      """## htsjdk.samtools.metrics.StringHeader
# CollectWgsMetrics  --INPUT /private/var/folders/8g/8hnt4z2j5lq8m0tqqlddx7891x22q8/T/tasks2018_10_29_09_23_56/tasks2018_10_29_09_23_568186860400775175580.temp/some.bam --OUTPUT /var/folders/8g/8hnt4z2j5lq8m0tqqlddx7891x22q8/T/fileutil2018_10_29_09_23_570/fileutil2018_10_29_09_23_572368374797485347218.qc --COUNT_UNPAIRED true --READ_LENGTH 150 --REFERENCE_SEQUENCE /private/var/folders/8g/8hnt4z2j5lq8m0tqqlddx7891x22q8/T/tasks2018_10_29_09_23_56/tasks2018_10_29_09_23_568186860400775175580.temp/referenceFasta.fasta  --MINIMUM_MAPPING_QUALITY 20 --MINIMUM_BASE_QUALITY 20 --COVERAGE_CAP 250 --LOCUS_ACCUMULATION_CAP 100000 --STOP_AFTER -1 --INCLUDE_BQ_HISTOGRAM false --SAMPLE_SIZE 10000 --ALLELE_FRACTION 0.001 --ALLELE_FRACTION 0.005 --ALLELE_FRACTION 0.01 --ALLELE_FRACTION 0.02 --ALLELE_FRACTION 0.05 --ALLELE_FRACTION 0.1 --ALLELE_FRACTION 0.2 --ALLELE_FRACTION 0.3 --ALLELE_FRACTION 0.5 --USE_FAST_ALGORITHM false --VERBOSITY INFO --QUIET false --VALIDATION_STRINGENCY STRICT --COMPRESSION_LEVEL 5 --MAX_RECORDS_IN_RAM 500000 --CREATE_INDEX false --CREATE_MD5_FILE false --GA4GH_CLIENT_SECRETS client_secrets.json --help false --version false --showHidden false --USE_JDK_DEFLATER false --USE_JDK_INFLATER false
## htsjdk.samtools.metrics.StringHeader
# Started on: Mon Oct 29 09:23:58 CET 2018

## METRICS CLASS	
GENOME_TERRITORY	MEAN_COVERAGE	SD_COVERAGE	MEDIAN_COVERAGE	MAD_COVERAGE	PCT_EXC_MAPQ	PCT_EXC_DUPE	PCT_EXC_UNPAIRED	PCT_EXC_BASEQ	PCT_EXC_OVERLAP	PCT_EXC_CAPPED	PCT_EXC_TOTAL	PCT_1X	PCT_5X	PCT_10X	PCT_15X	PCT_20X	PCT_25X	PCT_30X	PCT_40X	PCT_50X	PCT_60X	PCT_70X	PCT_80X	PCT_90X	PCT_100X	HET_SNP_SENSITIVITY	HET_SNP_Q
55809553	0	0	0	0	0.7944	0	0	0.2056	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0.000104	0

## HISTOGRAM	java.lang.Integer
coverage	high_quality_coverage_count
0	55809553
1	0
2	0
3	0
4	0
5	0
6	0
7	0
8	0
9	0
10	0
11	0
12	0
13	0
14	0
15	0
16	0
17	0
18	0
19	0
20	0
21	0
22	0
23	0
24	0
25	0
26	0
27	0
28	0
29	0
30	0
31	0
32	0
33	0
34	0
35	0
36	0
37	0
38	0
39	0
40	0
41	0
42	0
43	0
44	0
45	0
46	0
47	0
48	0
49	0
50	0
51	0
52	0
53	0
54	0
55	0
56	0
57	0
58	0
59	0
60	0
61	0
62	0
63	0
64	0
65	0
66	0
67	0
68	0
69	0
70	0
71	0
72	0
73	0
74	0
75	0
76	0
77	0
78	0
79	0
80	0
81	0
82	0
83	0
84	0
85	0
86	0
87	0
88	0
89	0
90	0
91	0
92	0
93	0
94	0
95	0
96	0
97	0
98	0
99	0
100	0
101	0
102	0
103	0
104	0
105	0
106	0
107	0
108	0
109	0
110	0
111	0
112	0
113	0
114	0
115	0
116	0
117	0
118	0
119	0
120	0
121	0
122	0
123	0
124	0
125	0
126	0
127	0
128	0
129	0
130	0
131	0
132	0
133	0
134	0
135	0
136	0
137	0
138	0
139	0
140	0
141	0
142	0
143	0
144	0
145	0
146	0
147	0
148	0
149	0
150	0
151	0
152	0
153	0
154	0
155	0
156	0
157	0
158	0
159	0
160	0
161	0
162	0
163	0
164	0
165	0
166	0
167	0
168	0
169	0
170	0
171	0
172	0
173	0
174	0
175	0
176	0
177	0
178	0
179	0
180	0
181	0
182	0
183	0
184	0
185	0
186	0
187	0
188	0
189	0
190	0
191	0
192	0
193	0
194	0
195	0
196	0
197	0
198	0
199	0
200	0
201	0
202	0
203	0
204	0
205	0
206	0
207	0
208	0
209	0
210	0
211	0
212	0
213	0
214	0
215	0
216	0
217	0
218	0
219	0
220	0
221	0
222	0
223	0
224	0
225	0
226	0
227	0
228	0
229	0
230	0
231	0
232	0
233	0
234	0
235	0
236	0
237	0
238	0
239	0
240	0
241	0
242	0
243	0
244	0
245	0
246	0
247	0
248	0
249	0
250	0

"""

    val variantCallingMetrics =
      """## htsjdk.samtools.metrics.StringHeader
# CollectVariantCallingMetrics  --INPUT /private/var/folders/8g/8hnt4z2j5lq8m0tqqlddx7891x22q8/T/tasks2018_11_07_14_50_14/tasks2018_11_07_14_50_142379980182653815646.temp/some.vcf.gz --OUTPUT /var/folders/8g/8hnt4z2j5lq8m0tqqlddx7891x22q8/T/fileutil2018_11_07_14_50_150/fileutil2018_11_07_14_50_155949749377302404169.metrics --DBSNP /private/var/folders/8g/8hnt4z2j5lq8m0tqqlddx7891x22q8/T/tasks2018_11_07_14_50_14/tasks2018_11_07_14_50_142379980182653815646.temp/dbsnp.vcf --TARGET_INTERVALS /var/folders/8g/8hnt4z2j5lq8m0tqqlddx7891x22q8/T/fileutil2018_11_07_14_50_150/fileutil2018_11_07_14_50_153973803142837295883 --SEQUENCE_DICTIONARY /private/var/folders/8g/8hnt4z2j5lq8m0tqqlddx7891x22q8/T/tasks2018_11_07_14_50_14/tasks2018_11_07_14_50_142379980182653815646.temp/referenceFasta.dict --GVCF_INPUT true  --THREAD_COUNT 1 --VERBOSITY INFO --QUIET false --VALIDATION_STRINGENCY STRICT --COMPRESSION_LEVEL 5 --MAX_RECORDS_IN_RAM 500000 --CREATE_INDEX false --CREATE_MD5_FILE false --GA4GH_CLIENT_SECRETS client_secrets.json --help false --version false --showHidden false --USE_JDK_DEFLATER false --USE_JDK_INFLATER false
## htsjdk.samtools.metrics.StringHeader
# Started on: Wed Nov 07 14:50:17 CET 2018

## METRICS CLASS	picard.vcf.CollectVariantCallingMetrics$VariantCallingSummaryMetrics
TOTAL_SNPS	NUM_IN_DB_SNP	NOVEL_SNPS	FILTERED_SNPS	PCT_DBSNP	DBSNP_TITV	NOVEL_TITV	TOTAL_INDELS	NOVEL_INDELS	FILTERED_INDELS	PCT_DBSNP_INDELS	NUM_IN_DB_SNP_INDELS	DBSNP_INS_DEL_RATIO	NOVEL_INS_DEL_RATIO	TOTAL_MULTIALLELIC_SNPS	NUM_IN_DB_SNP_MULTIALLELIC	TOTAL_COMPLEX_INDELS	NUM_IN_DB_SNP_COMPLEX_INDELS	SNP_REFERENCE_BIAS	NUM_SINGLETONS
0	0	0	0	?	0	0	0	0	0	?	0	0	0	0	0	0	0	?	0


"""

    val insertSizeMetricsText =
      """## htsjdk.samtools.metrics.StringHeader
# CollectMultipleMetrics  --INPUT /private/var/folders/8g/8hnt4z2j5lq8m0tqqlddx7891x22q8/T/tasks2018_11_23_18_04_19/tasks2018_11_23_18_04_19513054381560356401.temp/some.bam --ASSUME_SORTED true --OUTPUT /var/folders/8g/8hnt4z2j5lq8m0tqqlddx7891x22q8/T/fileutil2018_11_23_18_04_210/fileutil2018_11_23_18_04_21827496703813495769.qc --METRIC_ACCUMULATION_LEVEL READ_GROUP --METRIC_ACCUMULATION_LEVEL ALL_READS --PROGRAM CollectAlignmentSummaryMetrics --PROGRAM CollectInsertSizeMetrics --PROGRAM CollectSequencingArtifactMetrics --REFERENCE_SEQUENCE /private/var/folders/8g/8hnt4z2j5lq8m0tqqlddx7891x22q8/T/tasks2018_11_23_18_04_19/tasks2018_11_23_18_04_19513054381560356401.temp/referenceFasta.fasta  --STOP_AFTER 0 --INCLUDE_UNPAIRED false --VERBOSITY INFO --QUIET false --VALIDATION_STRINGENCY STRICT --COMPRESSION_LEVEL 5 --MAX_RECORDS_IN_RAM 500000 --CREATE_INDEX false --CREATE_MD5_FILE false --GA4GH_CLIENT_SECRETS client_secrets.json --help false --version false --showHidden false --USE_JDK_DEFLATER false --USE_JDK_INFLATER false
## htsjdk.samtools.metrics.StringHeader
# Started on: Fri Nov 23 18:04:44 CET 2018

## METRICS CLASS	picard.analysis.InsertSizeMetrics
MEDIAN_INSERT_SIZE	MODE_INSERT_SIZE	MEDIAN_ABSOLUTE_DEVIATION	MIN_INSERT_SIZE	MAX_INSERT_SIZE	MEAN_INSERT_SIZE	STANDARD_DEVIATION	READ_PAIRS	PAIR_ORIENTATION	WIDTH_OF_10_PERCENT	WIDTH_OF_20_PERCENT	WIDTH_OF_30_PERCENT	WIDTH_OF_40_PERCENT	WIDTH_OF_50_PERCENT	WIDTH_OF_60_PERCENT	WIDTH_OF_70_PERCENT	WIDTH_OF_80_PERCENT	WIDTH_OF_90_PERCENT	WIDTH_OF_95_PERCENT	WIDTH_OF_99_PERCENT	SAMPLE	LIBRARY	READ_GROUP
501	519	33	302	682	500.1382	48.971636	5000	FR	13	27	37	53	67	83	103	127	161	191	251			
501	519	33	302	682	500.1382	48.971636	5000	FR	13	27	37	53	67	83	103	127	161	191	251	someProject.someSampleId	someProject.someSampleId	someRunId.1

## HISTOGRAM	java.lang.Integer
insert_size	All_Reads.fr_count	someRunId.1.fr_count
302	1	1
338	1	1
339	1	1
343	1	1
345	2	2
347	1	1
349	1	1
350	1	1
351	1	1
352	1	1
353	1	1
355	2	2
357	1	1
358	2	2
361	3	3
363	1	1
366	1	1
367	1	1
368	2	2
369	1	1
370	1	1
371	1	1
372	2	2
373	2	2
375	1	1
376	1	1
377	4	4
379	1	1
380	1	1
381	2	2
382	2	2
383	4	4
385	1	1
386	6	6
388	1	1
389	6	6
390	3	3
391	4	4
392	2	2
393	7	7
394	3	3
395	3	3
397	7	7
398	4	4
399	2	2
400	4	4
401	6	6
402	7	7
403	5	5
404	8	8
405	5	5
406	6	6
407	9	9
408	9	9
409	10	10
410	7	7
411	3	3
412	10	10
413	8	8
414	13	13
415	8	8
416	8	8
417	11	11
418	8	8
419	11	11
420	14	14
421	19	19
422	10	10
423	10	10
424	10	10
425	11	11
426	16	16
427	12	12
428	16	16
429	17	17
430	24	24
431	13	13
432	21	21
433	15	15
434	13	13
435	17	17
436	16	16
437	15	15
438	20	20
439	19	19
440	27	27
441	20	20
442	17	17
443	17	17
444	20	20
445	19	19
446	22	22
447	20	20
448	19	19
449	30	30
450	22	22
451	24	24
452	21	21
453	26	26
454	27	27
455	22	22
456	27	27
457	27	27
458	27	27
459	27	27
460	31	31
461	27	27
462	36	36
463	27	27
464	31	31
465	29	29
466	34	34
467	30	30
468	29	29
469	33	33
470	39	39
471	37	37
472	34	34
473	24	24
474	30	30
475	33	33
476	39	39
477	36	36
478	26	26
479	34	34
480	32	32
481	33	33
482	33	33
483	43	43
484	43	43
485	26	26
486	41	41
487	36	36
488	50	50
489	30	30
490	39	39
491	44	44
492	36	36
493	35	35
494	36	36
495	46	46
496	49	49
497	40	40
498	35	35
499	37	37
500	34	34
501	47	47
502	28	28
503	34	34
504	46	46
505	51	51
506	48	48
507	41	41
508	43	43
509	49	49
510	43	43
511	26	26
512	42	42
513	35	35
514	41	41
515	48	48
516	50	50
517	38	38
518	39	39
519	57	57
520	47	47
521	40	40
522	34	34
523	30	30
524	35	35
525	35	35
526	39	39
527	32	32
528	27	27
529	45	45
530	34	34
531	30	30
532	38	38
533	25	25
534	37	37
535	31	31
536	38	38
537	24	24
538	28	28
539	32	32
540	31	31
541	28	28
542	27	27
543	26	26
544	23	23
545	28	28
546	22	22
547	37	37
548	24	24
549	27	27
550	24	24
551	16	16
552	20	20
553	22	22
554	26	26
555	21	21
556	18	18
557	27	27
558	23	23
559	23	23
560	12	12
561	22	22
562	18	18
563	15	15
564	18	18
565	24	24
566	18	18
567	10	10
568	19	19
569	14	14
570	14	14
571	15	15
572	12	12
573	21	21
574	11	11
575	15	15
576	10	10
577	15	15
578	20	20
579	9	9
580	13	13
581	10	10
582	12	12
583	9	9
584	9	9
585	9	9
586	6	6
587	1	1
588	11	11
589	9	9
590	4	4
591	10	10
592	8	8
593	5	5
594	8	8
595	10	10
596	5	5
597	6	6
598	5	5
599	8	8
600	3	3
601	6	6
602	5	5
603	7	7
605	3	3
606	3	3
607	3	3
608	5	5
609	3	3
610	2	2
611	5	5
612	2	2
614	4	4
615	2	2
616	5	5
617	1	1
618	3	3
619	2	2
620	2	2
621	2	2
622	1	1
623	1	1
624	3	3
625	1	1
627	1	1
628	1	1
630	2	2
631	1	1
632	3	3
634	1	1
635	1	1
639	1	1
641	1	1
643	1	1
647	1	1
648	1	1
660	1	1
682	1	1

"""

    val fastpText = scala.io.Source
      .fromInputStream(
        getClass.getResourceAsStream(
          "/someProject.someSampleId.someRunId.L001.fastp.json"))
      .mkString

    val project = Project("someProject")
    val sampleId = SampleId("someSampleId")
    val runId = RunId("someRunId")
    val lane = Lane(1)
  }
}
