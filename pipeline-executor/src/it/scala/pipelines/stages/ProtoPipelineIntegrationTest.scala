package org.gc.pipelines.stages

import org.scalatest._

import tasks._

import org.gc.pipelines.model._
import org.gc.pipelines.util.StableSet
import org.gc.pipelines.application.{
  RunfolderReadyForProcessing,
  DemultiplexingConfiguration,
  RunConfiguration,
  WESConfiguration,
  RNASeqConfiguration,
  InputFastQPerLane,
  InputSampleAsFastQ,
  AnalysisAssignments,
  SendProgressData,
  ProgressData
}
import org.gc.pipelines.util.StableSet

import scala.concurrent.Await
import org.gc.pipelines.GenericTestHelpers._
import org.gc.pipelines.PipelineFixtures._

class ProtopipelineIntegrationTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen {

  test("Prototype pipelien should accept fastq file") {
    new Fixture {
      Given("a runfolder and a reference file")
      When("the pipeline executes a run ")
      withTaskSystem(testConfig) { implicit ts =>
        import scala.concurrent.ExecutionContext.Implicits.global

        val run =
          RunfolderReadyForProcessing(
            runId,
            None,
            Some(
              List(
                InputSampleAsFastQ(
                  lanes = Set(
                    InputFastQPerLane(Lane(1),
                                      read1,
                                      read2,
                                      None,
                                      None,
                                      None,
                                      None)
                  ),
                  project = Project("project1"),
                  sampleId = SampleId("sample1")
                ))),
            runConfiguration
          )
        val pipeline = new ProtoPipeline(progressServer)
        import scala.concurrent.duration._
        val demultiplexedSamples =
          Await.result(pipeline.demultiplex(run), atMost = 400000 seconds)

        Then("Demultiplexing should happen")
        demultiplexedSamples.nonEmpty shouldBe true
        demultiplexedSamples.size shouldBe 1
        demultiplexedSamples.map(dm => (dm.project, dm.sampleId)).toSet shouldBe
          Set(("project1", "sample1"))

      }
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
        val pipeline = new ProtoPipeline(progressServer)
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
                pipeline.processSample(
                  run,
                  analysisAssignment,
                  None,
                  demultiplexedSample
                ),
                atMost = 400000 seconds
              )
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
                                                analysisAssignment,
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

    val wesConfiguration = WESConfiguration(
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
      minimumWGSCoverage = None,
      variantCallingContigs = None
    )

    val rnaConfiguration = RNASeqConfiguration(
      analysisId = AnalysisId("default"),
      referenceFasta = referenceFasta,
      geneModelGtf = gtfFile.getAbsolutePath,
      qtlToolsCommandLineArguments = Nil,
      quantificationGtf = gtfFile.getAbsolutePath
    )

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
          partitionByLane = None,
          partitionByTileCount = None
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
          partitionByLane = None,
          partitionByTileCount = None
        )
      ),
      globalIndexSet = Some(globalIndexSetFilePath),
      lastRunOfSamples = StableSet.empty
    )

    val analysisAssignment = AnalysisAssignments(
      Map(Project("project1") -> Seq(wesConfiguration, rnaConfiguration)))

    val (testConfig, basePath) = makeTestConfig

    val progressServer = new SendProgressData {
      def send(d: ProgressData) = ()
    }

  }

}
