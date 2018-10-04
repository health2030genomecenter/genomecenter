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

  test("Parse HsMetrics") {
    new Fixture {
      Given("an output table from picard's HsMetrics")
      When("we try to parse it")
      HsMetrics
        .Root(hsMetricsFile, project, sampleId, runId)
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
              CPUMemoryRequest(1, 3000))
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
            qcMetrics <- AlignmentQC.general(input)(CPUMemoryRequest(1, 3000))
          } yield qcMetrics

        await(future.flatMap(_.alignmentSummary.file))
      }

      Then("at least the alignment summary metrics file should be generated")
      result.get.canRead shouldBe true

    }
  }

  trait Fixture {

    val bed = new File(
      getClass.getResource("/tutorial_8017/capture.bed").getFile)
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

    val hsMetricsFile =
      """## htsjdk.samtools.metrics.StringHeader
# CollectHsMetrics  --BAIT_INTERVALS /var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/fileutil2018_10_04_14_42_150/fileutil2018_10_04_14_42_154101170785414879512 --BAIT_SET_NAME some.bed --TARGET_INTERVALS /var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/fileutil2018_10_04_14_42_150/fileutil2018_10_04_14_42_154101170785414879512 --INPUT /private/var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/tasks2018_10_04_14_42_11/tasks2018_10_04_14_42_118573417766713096981.temp/some.bam --OUTPUT /var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/fileutil2018_10_04_14_42_150/fileutil2018_10_04_14_42_151781444983663316224.qc --METRIC_ACCUMULATION_LEVEL READ_GROUP --METRIC_ACCUMULATION_LEVEL ALL_READS --REFERENCE_SEQUENCE /private/var/folders/l1/wh6226rn1fsbp5134w8391440000gn/T/tasks2018_10_04_14_42_11/tasks2018_10_04_14_42_118573417766713096981.temp/referenceFasta.fasta  --NEAR_DISTANCE 250 --MINIMUM_MAPPING_QUALITY 20 --MINIMUM_BASE_QUALITY 20 --CLIP_OVERLAPPING_READS true --COVERAGE_CAP 200 --SAMPLE_SIZE 10000 --ALLELE_FRACTION 0.001 --ALLELE_FRACTION 0.005 --ALLELE_FRACTION 0.01 --ALLELE_FRACTION 0.02 --ALLELE_FRACTION 0.05 --ALLELE_FRACTION 0.1 --ALLELE_FRACTION 0.2 --ALLELE_FRACTION 0.3 --ALLELE_FRACTION 0.5 --VERBOSITY INFO --QUIET false --VALIDATION_STRINGENCY STRICT --COMPRESSION_LEVEL 5 --MAX_RECORDS_IN_RAM 500000 --CREATE_INDEX false --CREATE_MD5_FILE false --GA4GH_CLIENT_SECRETS client_secrets.json --help false --version false --showHidden false --USE_JDK_DEFLATER false --USE_JDK_INFLATER false
## htsjdk.samtools.metrics.StringHeader
# Started on: Thu Oct 04 14:42:17 CEST 2018

## METRICS CLASS	picard.analysis.directed.HsMetrics
BAIT_SET	GENOME_SIZE	BAIT_TERRITORY	TARGET_TERRITORY	BAIT_DESIGN_EFFICIENCY	TOTAL_READS	PF_READS	PF_UNIQUE_READS	PCT_PF_READS	PCT_PF_UQ_READS	PF_UQ_READS_ALIGNED	PCT_PF_UQ_READS_ALIGNED	PF_BASES_ALIGNED	PF_UQ_BASES_ALIGNED	ON_BAIT_BASES	NEAR_BAIT_BASES	OFF_BAIT_BASES	ON_TARGET_BASES	PCT_SELECTED_BASES	PCT_OFF_BAIT	ON_BAIT_VS_SELECTED	MEAN_BAIT_COVERAGE	MEAN_TARGET_COVERAGE	MEDIAN_TARGET_COVERAGE	MAX_TARGET_COVERAGE	PCT_USABLE_BASES_ON_BAIT	PCT_USABLE_BASES_ON_TARGET	FOLD_ENRICHMENT	ZERO_CVG_TARGETS_PCT	PCT_EXC_DUPE	PCT_EXC_MAPQ	PCT_EXC_BASEQ	PCT_EXC_OVERLAP	PCT_EXC_OFF_TARGET	FOLD_80_BASE_PENALTY	PCT_TARGET_BASES_1X	PCT_TARGET_BASES_2X	PCT_TARGET_BASES_10X	PCT_TARGET_BASES_20X	PCT_TARGET_BASES_30X	PCT_TARGET_BASES_40X	PCT_TARGET_BASES_50X	PCT_TARGET_BASES_100X	HS_LIBRARY_SIZE	HS_PENALTY_10X	HS_PENALTY_20X	HS_PENALTY_30X	HS_PENALTY_40X	HS_PENALTY_50X	HS_PENALTY_100X	AT_DROPOUT	GC_DROPOUT	HET_SNP_SENSITIVITY	HET_SNP_Q	SAMPLE	LIBRARY	READ_GROUP
some.bed	58660772	58617616	58617616	1	10000	10000	10000	1	1	10000	1	1510000	1510000	931066	0	578934	0	0.6166	0.3834	1	0.015884	0	0	0	0.6166	0	0.617054	1	0	0.7944	0.2056	0	0.2056	?	0	0	0	0	0	0	0	0		0	0	0	0	0	0	0	0	0.000099	0			
some.bed	58660772	58617616	58617616	1	10000	10000	10000	1	1	10000	1	1510000	1510000	931066	0	578934	0	0.6166	0.3834	1	0.015884	0	0	0	0.6166	0	0.617054	1	0	0.7944	0.2056	0	0.2056	?	0	0	0	0	0	0	0	0		0	0	0	0	0	0	0	0	0.000099	0	someProject.someSampleId	someProject.someSampleId	someRunId.L001

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

    val project = Project("someProject")
    val sampleId = SampleId("someSampleId")
    val runId = RunId("someRunId")
    val lane = Lane("L001")
  }
}
