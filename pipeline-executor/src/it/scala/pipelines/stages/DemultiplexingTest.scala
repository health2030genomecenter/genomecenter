package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import fileutils.TempFile
import java.io.File

import org.gc.pipelines.util.Exec
import org.gc.pipelines.model._

import TestHelpers._
import org.gc.pipelines.GenericTestHelpers._

object Only extends Tag("only")

class DemultiplexingTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen {

  test("demultiplexing stats should deserialize") {

    val rawStats = io.circe.parser
      .decode[DemultiplexingStats.Root](statsFileContent)
      .right
      .get
    val summaryStat =
      DemultiplexingSummary.fromStats(rawStats, Map(), Set.empty)
    println(DemultiplexingSummary.renderAsTable(summaryStat))
  }

  test("index swap is reported") {

    val rawStats = io.circe.parser
      .decode[DemultiplexingStats.Root](statsFileContent)
      .right
      .get
    val summaryStat =
      DemultiplexingSummary.fromStats(rawStats, Map(), Set("GCCAAT"))
    summaryStat.laneSummaries.head.indexSwaps.head shouldBe DemultiplexingSummary
      .IndexSwap("GCCAAT", 140860, 0.27249970014586455, List())
  }

  test("Demultiplexing stage should demultiplex a simple run") {
    new Fixture {

      Given("a runfolder")
      When("executing the Demultiplexing.allLanes task")
      val result = withTaskSystem(testConfig) { implicit ts =>
        val future = Demultiplexing.allLanes(
          DemultiplexingInput(
            runFolderPath = runFolderPath,
            sampleSheet = SampleSheetFile(
              await(SharedFile(sampleSheetFile, "sampleSheet"))),
            extraBcl2FastqCliArguments = Seq("--tiles",
                                             "s_1_1101",
                                             "--use-bases-mask",
                                             "y75n,i6n*,n10,y75n"),
            globalIndexSet = None,
            partitionByLane = None,
            noPartition = None,
            partitionByTileCount = None
          ))(ResourceRequest(1, 500))
        import scala.concurrent.duration._
        scala.concurrent.Await.result(future, atMost = 400000 seconds)

      }

      Then(
        "a run and lane specific folder should be created at the root of the storage")
      // val outputFolder =
      // new File(basePath.getAbsolutePath + s"/demultiplex/$runId/all/1")
      // outputFolder.canRead shouldBe true

      And("uncaptured output files from bcl2fastq should be present")
      // val statsFolder = new File(outputFolder.getAbsolutePath + "/Stats")
      // statsFolder.canRead shouldBe true
      // val demultiplexStatFile =
      // new File(statsFolder.getAbsolutePath + "/DemuxSummaryF1L1.txt")
      // demultiplexStatFile.canRead shouldBe true

      And(
        "captured fastq files should be present for the demultiplexed samples and for the undetermined reads")
      result.get.fastqs.size shouldBe 4
      result.get.fastqs.toSeq.map(_.sampleId).toSet shouldBe Set("Undetermined",
                                                                 "GIB")
      result.get.fastqs.toSeq.map(_.readType).toSet shouldBe Set(2, 1)
      result.get.fastqs.toSeq.map(_.lane).toSet shouldBe Set(1)

    }
  }

  test("Demultiplexing stage should demultiplex a 10X run", Only) {
    new Fixture {

      Given("a runfolder")
      When("executing the Demultiplexing.allLanes task")
      val (demultiplexed, concatenated) = withTaskSystem(testConfig) {
        implicit ts =>
          implicit val ec = scala.concurrent.ExecutionContext.global
          val originalSamplesheet =
            SampleSheetFile(
              await(SharedFile(tenXsampleSheetFile, "sampleSheet")))
          val resolvedSamplesheet = await(
            ProtoPipelineStages.resolve10XIfNeeded(true, originalSamplesheet))
          val future = Demultiplexing.allLanes(
            DemultiplexingInput(
              runFolderPath = tenXRunFolderPath,
              sampleSheet = resolvedSamplesheet,
              extraBcl2FastqCliArguments = Seq(
                "--use-bases-mask",
                "y26,i8,y98",
                "--minimum-trimmed-read-length=8",
                "--mask-short-adapter-reads=8",
                "--ignore-missing-positions",
                "--ignore-missing-controls",
                "--ignore-missing-filter",
                "--ignore-missing-bcls"
              ),
              globalIndexSet = None,
              partitionByLane = None,
              noPartition = Some(true),
              partitionByTileCount = None
            ))(ResourceRequest(1, 500))
          import scala.concurrent.duration._
          val demultiplexed =
            scala.concurrent.Await.result(future, atMost = 400000 seconds)

          val perSampleFastQs = ProtoPipelineStages
            .groupBySample(demultiplexed.withoutUndetermined,
                           (1, 2),
                           None,
                           RunId("whatever"),
                           true)

          val concatenated = scala.concurrent.Await.result(
            TenXStages.concatenateFastQ(perSampleFastQs.head.withoutRunId)(
              ResourceRequest(1, 500)),
            atMost = 3000 seconds)

          (demultiplexed, concatenated)
      }.get

      And(
        "captured fastq files should be present for the demultiplexed samples")
      demultiplexed.fastqs.size shouldBe 10
      concatenated.lanes.size shouldBe 1
      concatenated.lanes.toSeq.head.read1.file.byteSize.toInt shouldBe 2916

    }
  }

  trait Fixture {
    def extract10XRunFolderTestData = {
      val tmpFolder = TempFile.createTempFolder(".runfolder_test")
      val testDataTarFile =
        getClass
          .getResource("/cellranger-tiny-bcl-1.2.0.tar.gz")
          .getFile
      Exec.bash("test.demultiplex")(
        s"cd ${tmpFolder.getAbsolutePath} && tar xf $testDataTarFile ")
      new File(tmpFolder, "cellranger-tiny-bcl-1.2.0").getAbsolutePath
    }
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
[Data],,,,,,,,,,
Sample_ID,Sample_Name,Sample_Plate,Sample_Well,Index_Plate_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description,Lane
GIB,GIB,,,F01,AD007,CAGATC,MolBC,NNNNNNNNNN,,,001
      """
    )
    val sampleSheetFile =
      fileutils.openFileWriter(_.write(sampleSheet.sampleSheetContent))._1

    val tenXsampleSheetFile = new File(
      getClass
        .getResource("/cellranger-tiny-bcl-samplesheet-1.2.0.csv")
        .getFile)
    val runFolderPath = extractRunFolderTestData

    val tenXRunFolderPath = extract10XRunFolderTestData

    val (testConfig, basePath) = makeTestConfig
  }
}
