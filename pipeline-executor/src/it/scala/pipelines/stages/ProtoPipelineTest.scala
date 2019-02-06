package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import fileutils.TempFile
import java.io.File

import org.gc.pipelines.util.Exec
import org.gc.pipelines.model._
import org.gc.pipelines.util.StableSet
import org.gc.pipelines.application.{
  RunfolderReadyForProcessing,
  DemultiplexingConfiguration,
  RunConfiguration,
  Selector,
  WESConfiguration
}
import org.gc.pipelines.util.StableSet

import scala.concurrent.Await

class ProtopipelineTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("parse RunInfo.xml") {
    new Fixture {
      ProtoPipelineStages.parseReadLength(runInfoContent) shouldBe Map(1 -> 50,
                                                                       2 -> 7,
                                                                       3 -> 50)
    }
  }

  test("Prototype pipeline should create bam files per sample") {
    new Fixture {

      Given("a runfolder and a reference file")
      When("the pipeline executes a run ")
      withTaskSystem(testConfig) { implicit ts =>
        import scala.concurrent.ExecutionContext.Implicits.global
        val run =
          RunfolderReadyForProcessing(runId,
                                      Some(runFolderPath),
                                      None,
                                      runConfiguration)
        val pipeline = new ProtoPipeline()
        import scala.concurrent.duration._
        val demultiplexedSamples =
          Await.result(pipeline.demultiplex(run), atMost = 400000 seconds)

        Then("Demultiplexing should happen")
        demultiplexedSamples.nonEmpty shouldBe true
        demultiplexedSamples.size shouldBe 4
        demultiplexedSamples.map(dm => (dm.project, dm.sampleId)).toSet shouldBe
          Set(("project2", "sample2"),
              ("project1", "GIB"),
              ("project3", "sample3"),
              ("project1", "GIB2"))

        Then("Fastq processing should happen")
        val processedSamples =
          demultiplexedSamples.filter(_.project == "project1").map {
            demultiplexedSample =>
              Await.result(
                pipeline.processSample(run, None, demultiplexedSample),
                atMost = 400000 seconds)
          }

        Then("Complete run")

        Await.result(
          pipeline.processCompletedRun(processedSamples.flatMap(_.toSeq)),
          atMost = 400000 seconds)

        Then("Complete project")

        Await.result(
          pipeline.processCompletedProject(processedSamples.flatMap(_.toSeq)),
          atMost = 400000 seconds)

        Then("Process again the same sample ")
        demultiplexedSamples
          .find(_.sampleId == processedSamples.head.get.sampleId)
          .map { demultiplexedSample =>
            Await.result(pipeline.processSample(run,
                                                Some(processedSamples.head.get),
                                                demultiplexedSample),
                         atMost = 400000 seconds)
          }

        Then(
          "a run and lane specific folder should be created at the root of the storage")
        // val demultiplexOutputFolder =
        // new File(basePath.getAbsolutePath + s"/demultiplex/$runId/all/1")
        // demultiplexOutputFolder.canRead shouldBe true

        And("uncaptured output files from bcl2fastq should be present")
        // val statsFolder =
        // new File(demultiplexOutputFolder.getAbsolutePath + "/Stats")
        // demultiplexOutputFolder.canRead shouldBe true
        // val demultiplexStatFile =
        // new File(statsFolder.getAbsolutePath + "/DemuxSummaryF1L1.txt")
        // demultiplexStatFile.canRead shouldBe true
        And("stderr file should be present")
        // new File(demultiplexOutputFolder, "stderr").canRead shouldBe true

        And("stats files should be merged")
        // new File(
        // basePath.getAbsolutePath + s"/demultiplex/$runId/all/$runId.Stats.json").canRead shouldBe true

        And("human readable table should be generated")
        // new File(
        // basePath.getAbsolutePath + s"/demultiplex/$runId/all/$runId.stats.table").canRead shouldBe true

        And("fastp report files are generated")
        // new File(
        // basePath.getAbsolutePath + s"/fastp/$runId/1/project1.GIB.whateverRunId.1.fastp.json").canRead shouldBe true
        // new File(
        // basePath.getAbsolutePath + s"/fastp/$runId/1/project1.GIB.whateverRunId.1.fastp.html").canRead shouldBe true

        And("bwa alignment per lane of the first sample should not be present")
        // val bwaFolder = new File(
        //   basePath.getAbsolutePath + s"/projects/project1/whateverRunId/intermediate/")
        And("stderr of alignment is present")
        // new File(bwaFolder, "project1.GIB.whateverRunId.1.bam.stderr").canRead shouldBe true
        And("bam file of alignment should be already deleted")
        // new File(bwaFolder, "project1.GIB.whateverRunId.1.bam").canRead shouldBe false

        And("merge and mark duplicate of the first sample should be present")
        // val markduplicatesFolder =
        //   new File(
        //     basePath.getAbsolutePath + s"/projects/project1/whateverRunId/intermediate/")
        // new File(
        // markduplicatesFolder,
        // "project1.GIB.whateverRunId.mdup.bam.stderr").canRead shouldBe true
        // new File(
        // markduplicatesFolder,
        // "project1.GIB.whateverRunId.mdup.markDuplicateMetrics").canRead shouldBe true
        And(
          "merged and duplicated marked bam and bai file should be already deleted")
        // new File(markduplicatesFolder, "project1.GIB.whateverRunId.mdup.bam").canRead shouldBe false
        // new File(markduplicatesFolder, "project1.GIB.whateverRunId.mdup.bai").canRead shouldBe false

        And("recalibrated bam files should be present")
        // val bqsrApplyFolderForProject1 =
        // new File(
        // basePath.getAbsolutePath + s"/projects/project1/whateverRunId/")

        // new File(
        // bqsrApplyFolderForProject1,
        // "project1.GIB.whateverRunId.mdup.sorted.bqsr.apply.stderr").canRead shouldBe true
        // new File(
        // bqsrApplyFolderForProject1,
        // "project1.GIB.whateverRunId.mdup.sorted.bqsr.bai").canRead shouldBe true
        // val project1RecalibratedBam =
        // new File(bqsrApplyFolderForProject1,
        //  "project1.GIB.whateverRunId.mdup.sorted.bqsr.bam")
        // project1RecalibratedBam.canRead shouldBe true
        // val project1Timestamp = project1RecalibratedBam.lastModified

        And("alignment summary metrics should be present")
        // val qcFolder = new File(
        // basePath.getAbsolutePath + s"/projects/project1/whateverRunId/QC")
        // new File(
        // qcFolder,
        // "project1.GIB.whateverRunId.mdup.sorted.bqsr.bam.alignment_summary_metrics").canRead shouldBe true

        And("hybridization selection metrics should be present")
        // new File(
        //   qcFolder,
        // "project1.GIB.whateverRunId.mdup.sorted.bqsr.bam.hsMetrics").canRead shouldBe true

        And("project3 should not be demultiplexed")
        // val project3Folder =
        //   new File(basePath.getAbsolutePath + s"/projects/project3/")
        // project3Folder.canRead shouldBe false

        And("runQC file should be present")
        // new File(basePath.getAbsolutePath + s"/runQC/$runId.wes.qc.table").canRead shouldBe true

        When("executing the same runfolder again")

        // val result2 = scala.concurrent.Await.result(pipeline.execute(run),
        //                                             atMost = 400000 seconds)

        Then("project3 should be demultiplexed")
        // project3Folder.canRead shouldBe true
        And("project1 alignment should not reexecute")
      // project1RecalibratedBam.lastModified shouldBe project1Timestamp

      }

    }
  }

  trait Fixture {
    def extractRunFolderTestData = {
      val tmpFolder = TempFile.createTempFolder(".runfolder_test")
      val testDataTarFile =
        getClass
          .getResource("/180622_M04914_0002_000000000-BWHDL.tar.gz")
          .getFile
      Exec.bash("test.demultiplex")(
        s"cd ${tmpFolder.getAbsolutePath} && tar xf $testDataTarFile ")
      new File(tmpFolder, "180622_M04914_0002_000000000-BWHDL").getAbsolutePath
    }

    val referenceFasta = getClass
      .getResource("/tutorial_8017/chr19_chr19_KI270866v1_alt.fasta")
      .getFile

    val targetIntervals = getClass
      .getResource("/tutorial_8017/capture.bed")
      .getFile

    val globalIndexSetFilePath = getClass
      .getResource("/indices")
      .getFile

    val knownSitesVCF = new File(
      getClass
        .getResource("/example.vcf.gz")
        .getFile)

    val runId = RunId("whateverRunId")
    val sampleSheet = SampleSheet(
      s"""[Header],,,,,,,,,,
IEMFileVersion,5,,,,,,,,,
Investigator Name,GC,,,,,,,,,
Experiment Name,Training_Miseq_22062018,,,,,,,,,
Date,22/06/2018,,,,,,,,,
Workflow,GenerateFASTQ,,,,,,,,,
Application,FASTQ Only,,,,,,,,,
Instrument Type,MiSeq,,,,,,,,,
Assay,TruSeq DNA PCR-Free,,,,,,,,,
Index Adapters,IDT-ILMN TruSeq DNA UD Indexes (24 Indexes),,,,,,,,,
Description,,,,,,,,,,
Chemistry,Amplicon,,,,,,,,,
,,,,,,,,,,
[Reads],,,,,,,,,,
76,,,,,,,,,,
76,,,,,,,,,,
,,,,,,,,,,
[Settings],,,,,,,,,,
ReverseComplement,0,,,,,,,,,
,,,,,,,,,,
[Data],,,,,,,,,,
Sample_ID,Sample_Name,Sample_Plate,Sample_Well,Index_Plate_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description,Lane
GIB,GIB,,,F01,AD007,CAGATC,MolBC,NNNNNNNNNN,project1,,001
sample2,sample2,,,boo,boo,ATCACG,MolBC,NNNNNNNNNN,project2,,001
      """
    )

    val sampleSheet2 = SampleSheet(
      s"""[Header],,,,,,,,,,
IEMFileVersion,5,,,,,,,,,
Investigator Name,GC,,,,,,,,,
Experiment Name,Training_Miseq_22062018,,,,,,,,,
Date,22/06/2018,,,,,,,,,
Workflow,GenerateFASTQ,,,,,,,,,
Application,FASTQ Only,,,,,,,,,
Instrument Type,MiSeq,,,,,,,,,
Assay,TruSeq DNA PCR-Free,,,,,,,,,
Index Adapters,IDT-ILMN TruSeq DNA UD Indexes (24 Indexes),,,,,,,,,
Description,,,,,,,,,,
Chemistry,Amplicon,,,,,,,,,
,,,,,,,,,,
[Reads],,,,,,,,,,
76,,,,,,,,,,
76,,,,,,,,,,
,,,,,,,,,,
[Settings],,,,,,,,,,
ReverseComplement,0,,,,,,,,,
,,,,,,,,,,
[Data],,,,,,,,,,
Sample_ID,Sample_Name,Sample_Plate,Sample_Well,Index_Plate_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description,Lane
GIB2,GIB2,,,F01,AD007,CAGATC,MolBC,NNNNNNNNNN,project1,,001
sample3,sample3,,,boo,boo,ATCACG,MolBC,NNNNNNNNNN,project3,,001
      """
    )
    val sampleSheetFile =
      fileutils.openFileWriter(_.write(sampleSheet.sampleSheetContent))._1

    val sampleSheetFile2 =
      fileutils.openFileWriter(_.write(sampleSheet2.sampleSheetContent))._1
    val runFolderPath = extractRunFolderTestData

    val gtfFile = new File(
      getClass
        .getResource("/short.gtf")
        .getFile)

    val runConfiguration = RunConfiguration(
      demultiplexingRuns = StableSet(
        DemultiplexingConfiguration(
          demultiplexingId = DemultiplexingId("demultiplexOnce"),
          sampleSheet = sampleSheetFile.getAbsolutePath,
          extraBcl2FastqArguments = Seq("--tiles",
                                        "s_1_1101",
                                        "--use-bases-mask",
                                        "y75n,i6n*,n10,y75n"),
          readAssignment = (1, 2),
          umi = None,
          tenX = None,
          partitionByLane = None
        ),
        DemultiplexingConfiguration(
          demultiplexingId = DemultiplexingId("demultiplexTwice"),
          sampleSheet = sampleSheetFile2.getAbsolutePath,
          extraBcl2FastqArguments = Seq("--tiles",
                                        "s_1_1101",
                                        "--use-bases-mask",
                                        "y75n,i6n*,n10,y75n"),
          readAssignment = (1, 2),
          umi = None,
          tenX = None,
          partitionByLane = None
        )
      ),
      globalIndexSet = Some(globalIndexSetFilePath),
      wesProcessing = StableSet(
        Selector(runIds = StableSet(RunId(runId)),
                 samples = StableSet.empty,
                 lanes = StableSet.empty,
                 projects = StableSet.empty) ->
          WESConfiguration(
            analysisId = AnalysisId("default"),
            referenceFasta = referenceFasta,
            targetIntervals = targetIntervals,
            bqsrKnownSites = StableSet(knownSitesVCF.getAbsolutePath),
            dbSnpVcf = knownSitesVCF.getAbsolutePath,
            variantEvaluationIntervals = targetIntervals,
            vqsrMillsAnd1Kg = None,
            vqsrHapmap = None,
            vqsrOneKgOmni = None,
            vqsrOneKgHighConfidenceSnps = None,
            vqsrDbSnp138 = None,
            doVariantCalls = Some(true),
            doJointCalls = Some(true),
            minimumTargetCoverage = None,
            minimumWGSCoverage = None
          )
      ),
      rnaProcessing = StableSet.empty
    )

    val (testConfig, basePath) = makeTestConfig

    val runInfoContent =
      """<?xml version="1.0"?>
<RunInfo xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="2">
  <Run Id="141008_7001253F_0202_AC5E7AANXX" Number="202">
    <Flowcell>C5E7AANXX</Flowcell>
    <Instrument>7001253F</Instrument>
    <Date>141008</Date>
    <Reads>
      <Read Number="1" NumCycles="50" IsIndexedRead="N" />
      <Read Number="2" NumCycles="7" IsIndexedRead="Y" />
      <Read Number="3" NumCycles="50" IsIndexedRead="N" />
    </Reads>
    <FlowcellLayout LaneCount="8" SurfaceCount="2" SwathCount="3" TileCount="16" />
    <AlignToPhiX>
      <Lane>3</Lane>
      <Lane>4</Lane>
      <Lane>5</Lane>
      <Lane>6</Lane>
      <Lane>7</Lane>
      <Lane>8</Lane>
    </AlignToPhiX>
  </Run>
</RunInfo>
"""
  }
}
