package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import fileutils.TempFile
import java.io.File

import org.gc.pipelines.util.Exec
import org.gc.pipelines.model._
import org.gc.pipelines.application.{
  RunfolderReadyForProcessing,
  RunConfiguration
}

class ProtopipelineTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("Prototype pipeline should create bam files per sample") {
    new Fixture {

      Given("a runfolder and a reference file")
      When("the pipeline executes a run ")
      withTaskSystem(testConfig) { implicit ts =>
        import scala.concurrent.ExecutionContext.Implicits.global
        val run =
          RunfolderReadyForProcessing(runId = runId,
                                      runFolderPath = runFolderPath,
                                      runConfiguration = runConfiguration)
        val pipeline = new ProtoPipeline()
        val future = pipeline.execute(run)
        import scala.concurrent.duration._
        val result =
          scala.concurrent.Await.result(future, atMost = 400000 seconds)

        result shouldBe true

        Then(
          "a run and lane specific folder should be created at the root of the storage")
        val demultiplexOutputFolder =
          new File(basePath.getAbsolutePath + s"/demultiplex/$runId/L001")
        demultiplexOutputFolder.canRead shouldBe true

        And("uncaptured output files from bcl2fastq should be present")
        val statsFolder =
          new File(demultiplexOutputFolder.getAbsolutePath + "/Stats")
        demultiplexOutputFolder.canRead shouldBe true
        val demultiplexStatFile =
          new File(statsFolder.getAbsolutePath + "/DemuxSummaryF1L1.txt")
        demultiplexStatFile.canRead shouldBe true
        And("stderr file should be present")
        new File(demultiplexOutputFolder, "stderr").canRead shouldBe true

        And("stats files should be merged")
        new File(
          basePath.getAbsolutePath + s"/demultiplex/$runId/$runId.Stats.json").canRead shouldBe true

        And("human readable table should be generated")
        new File(
          basePath.getAbsolutePath + s"/demultiplex/$runId/$runId.stats.table").canRead shouldBe true

        And("fastp report files are generated")
        new File(
          basePath.getAbsolutePath + s"/demultiplex/$runId/L001/project1.GIB.whateverRunId.L001.fastp.json").canRead shouldBe true
        new File(
          basePath.getAbsolutePath + s"/demultiplex/$runId/L001/project1.GIB.whateverRunId.L001.fastp.html").canRead shouldBe true

        And("bwa alignment per lane of the first sample should not be present")
        val bwaFolder = new File(
          basePath.getAbsolutePath + s"/projects/project1/whateverRunId/intermediate/")
        And("stderr of alignment is present")
        new File(bwaFolder, "project1.GIB.whateverRunId.L001.bam.stderr").canRead shouldBe true
        And("bam file of alignment should be already deleted")
        new File(bwaFolder, "project1.GIB.whateverRunId.L001.bam").canRead shouldBe false

        And("merge and mark duplicate of the first sample should be present")
        val markduplicatesFolder =
          new File(
            basePath.getAbsolutePath + s"/projects/project1/whateverRunId/intermediate/")
        new File(markduplicatesFolder, "project1.GIB.whateverRunId.bam.stderr").canRead shouldBe true
        new File(
          markduplicatesFolder,
          "project1.GIB.whateverRunId.markDuplicateMetrics").canRead shouldBe true
        And(
          "merged and duplicated marked bam and bai file should be already deleted")
        new File(markduplicatesFolder, "project1.GIB.whateverRunId.bam").canRead shouldBe false
        new File(markduplicatesFolder, "project1.GIB.whateverRunId.bai").canRead shouldBe false

        And("recalibrated bam files should be present")
        val bqsrApplyFolderForProject1 =
          new File(
            basePath.getAbsolutePath + s"/projects/project1/whateverRunId/")

        new File(
          bqsrApplyFolderForProject1,
          "project1.GIB.whateverRunId.bqsr.apply.stderr").canRead shouldBe true
        new File(bqsrApplyFolderForProject1,
                 "project1.GIB.whateverRunId.bqsr.bai").canRead shouldBe true
        val project1RecalibratedBam =
          new File(bqsrApplyFolderForProject1,
                   "project1.GIB.whateverRunId.bqsr.bam")
        project1RecalibratedBam.canRead shouldBe true
        val project1Timestamp = project1RecalibratedBam.lastModified

        And("alignment summary metrics should be present")
        val qcFolder = new File(
          basePath.getAbsolutePath + s"/projects/project1/whateverRunId/QC")
        new File(
          qcFolder,
          "project1.GIB.whateverRunId.bqsr.bam.alignment_summary_metrics").canRead shouldBe true

        And("hybridization selection metrics should be present")
        new File(qcFolder, "project1.GIB.whateverRunId.bqsr.bam.hsMetrics").canRead shouldBe true

        And("project3 should not be demultiplexed")
        val project3Folder =
          new File(basePath.getAbsolutePath + s"/projects/project3/")
        project3Folder.canRead shouldBe false

        And("runQC file should be present")
        new File(basePath.getAbsolutePath + s"/runQC/$runId.qc.table").canRead shouldBe true

        When("executing the same runfolder with a different sample sheet")

        scala.concurrent.Await.result(
          pipeline.execute(
            run.copy(runConfiguration = run.runConfiguration.copy(
              sampleSheet = sampleSheetFile2.getAbsolutePath))),
          atMost = 400000 seconds)

        Then("project3 should be demultiplexed")
        project3Folder.canRead shouldBe true
        And("project1 alignment should not reexecute")
        project1RecalibratedBam.lastModified shouldBe project1Timestamp

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

    val knownSitesVCF = new File(
      getClass
        .getResource("/example.vcf")
        .getFile)

    val runId = "whateverRunId"
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
GIB,GIB,,,F01,AD007,CAGATC,MolBC,NNNNNNNNNN,project1,,001
sample2,sample2,,,boo,boo,ATCACG,MolBC,NNNNNNNNNN,project3,,001
      """
    )
    val sampleSheetFile =
      fileutils.openFileWriter(_.write(sampleSheet.sampleSheetContent))._1

    val sampleSheetFile2 =
      fileutils.openFileWriter(_.write(sampleSheet2.sampleSheetContent))._1
    val runFolderPath = extractRunFolderTestData

    val runConfiguration = RunConfiguration(
      automatic = true,
      sampleSheet = sampleSheetFile.getAbsolutePath,
      referenceFasta = referenceFasta,
      targetIntervals = targetIntervals,
      bqsrKnownSites = Set(knownSitesVCF.getAbsolutePath),
      extraBcl2FastqArguments =
        Seq("--tiles", "s_1_1101", "--use-bases-mask", "y75n,i6n*,n10,y75n"),
      readAssignment = (1, 2),
      umi = None
    )

    val (testConfig, basePath) = makeTestConfig
  }
}
