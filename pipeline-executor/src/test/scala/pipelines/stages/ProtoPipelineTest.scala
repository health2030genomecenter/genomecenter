package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import fileutils.TempFile
import java.io.File

import org.gc.pipelines.util.Exec
import org.gc.pipelines.model._
import org.gc.pipelines.application.RunfolderReadyForProcessing

class ProtopipelineTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("Prototype pipeline should create bam files per sample") {
    new Fixture {

      Given("a runfolder and a reference file")
      When("the pipeline")
      val result = withTaskSystem(testConfig) { implicit ts =>
        import scala.concurrent.ExecutionContext.Implicits.global
        val run =
          RunfolderReadyForProcessing(runId = runId,
                                      sampleSheet = sampleSheet,
                                      runFolderPath = runFolderPath)
        val future = new ProtoPipeline().execute(run)
        import scala.concurrent.duration._
        scala.concurrent.Await.result(future, atMost = 400000 seconds)

      }

      result.get shouldBe true

      Then(
        "a run and lane specific folder should be created at the root of the storage")
      val demultiplexOutputFolder = new File(
        basePath.getAbsolutePath + s"/demultiplex/demultiplex-per-lane/$runId/L001")
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

      And("bwa alignment per lane of the first sample should be present")
      val bwaFolder = new File(
        basePath.getAbsolutePath + s"/bwa/bwa-persample/project1/GIB/bwa-perlane")
      And("stderr of alignment is present")
      new File(bwaFolder, "project1.GIB.whateverRunId.L001.stderr").canRead

      And("merge and mark duplicate of the first sample should be present")
      val markduplicatesFolder =
        new File(
          basePath.getAbsolutePath + s"/bwa/bwa-persample/project1/GIB/merge-markduplicate")
      new File(markduplicatesFolder, "project1.GIB.whateverRunId.stderr").canRead
      new File(markduplicatesFolder, "project1.GIB.whateverRunId.bam").canRead
      new File(markduplicatesFolder, "project1.GIB.whateverRunId.bai").canRead
      new File(markduplicatesFolder, "project1.GIB.whateverRunId.metrics").canRead

      And("recalibrated bam files should be present")
      val bqsrApplyFolderForProject1 =
        new File(
          basePath.getAbsolutePath + s"/bqsr/project1/bqsr-apply")

      new File(bqsrApplyFolderForProject1, "project1.GIB.whateverRunId.bqsr.apply.stderr").canRead
      new File(bqsrApplyFolderForProject1, "project1.GIB.whateverRunId.bqsr.bai").canRead
      new File(bqsrApplyFolderForProject1, "project1.GIB.whateverRunId.bqsr.bai").canRead

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
[GenomeCenter]
bcl2fastqArguments,["--tiles","s_1_1101","--use-bases-mask","y75n,i6n*,n10,y75n"]
automatic
referenceFasta,$referenceFasta
bqsr.knownSites,["$knownSitesVCF"]
[Data],,,,,,,,,,
Sample_ID,Sample_Name,Sample_Plate,Sample_Well,Index_Plate_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description,Lane
GIB,GIB,,,F01,AD007,CAGATC,MolBC,NNNNNNNNNN,project1,,L001
sample2,sample2,,,boo,boo,ATCACG,MolBC,NNNNNNNNNN,project2,,L001
      """
    )
    val runFolderPath = extractRunFolderTestData

    val (testConfig, basePath) = makeTestConfig
  }
}
