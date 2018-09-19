package org.gc.pipelines.stages

import org.scalatest._

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

import tasks._
import com.typesafe.config.ConfigFactory
import fileutils.TempFile
import java.io.File

import org.gc.pipelines.util.Exec
import org.gc.pipelines.model._
import org.gc.pipelines.application.RunfolderReadyForProcessing

class DemultiplexingTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("Demultiplexing stage should demultiplex a simple run") {
    new Fixture {

      Given("a runfolder")
      When("executing the Demultiplexing.allLanes task")
      val result = withTaskSystem(testConfig) { implicit ts =>
        val future = Demultiplexing.allLanes(
          RunfolderReadyForProcessing(runId = runId,
                                      sampleSheet = sampleSheet,
                                      runFolderPath = runFolderPath))(
          CPUMemoryRequest(1, 500))
        import scala.concurrent.duration._
        scala.concurrent.Await.result(future, atMost = 400000 seconds)

      }

      Then(
        "a run and lane specific folder should be created at the root of the storage")
      val outputFolder = new File(
        basePath.getAbsolutePath + s"/demultiplex/demultiplex-per-lane/$runId/L001")
      outputFolder.canRead shouldBe true

      And("uncaptured output files from bcl2fastq should be present")
      val statsFolder = new File(outputFolder.getAbsolutePath + "/Stats")
      statsFolder.canRead shouldBe true
      val demultiplexStatFile =
        new File(statsFolder.getAbsolutePath + "/DemuxSummaryF1L1.txt")
      demultiplexStatFile.canRead shouldBe true

      And(
        "captured fastq files should be present for the demultiplexed samples and for the undetermined reads")
      result.get.fastqs.size shouldBe 4
      result.get.fastqs.toList.map(_.sampleId).toSet shouldBe Set(
        "Undetermined",
        "GIB")
      result.get.fastqs.toList.map(_.readType).toSet shouldBe Set("R2", "R1")
      result.get.fastqs.toList.map(_.lane).toSet shouldBe Set("L001")
      And("the file history field should be filled in")
      result.get.fastqs.toList.map(_.fastq.file.history).forall(_.isDefined) shouldBe true

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

    val runId = "whateverRunId"
    val sampleSheet = SampleSheet(
      """[Header],,,,,,,,,,
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
[Data],,,,,,,,,,
Sample_ID,Sample_Name,Sample_Plate,Sample_Well,Index_Plate_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description,Lane
GIB,GIB,,,F01,AD007,CAGATC,MolBC,NNNNNNNNNN,,,L001
      """
    )
    val runFolderPath = extractRunFolderTestData

    val (testConfig, basePath) = makeTestConfig
  }
}

trait TestHelpers {

  def await[T](f: Future[T]) = Await.result(f, atMost = 60 seconds)

  def makeTestConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    val config = ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=12
      """
    )
    (config, tmp)
  }

}
