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
          .foreach { demultiplexedSample =>
            Await.result(pipeline.processSample(run,
                                                analysisAssignment,
                                                Some(processedSamples.head.get),
                                                demultiplexedSample),
                         atMost = 400000 seconds)
          }

       

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
      variantCallingContigs = None,
      singleSampleVqsr = None,
      keepVcf = Some(true),
      mergeSingleCalls = Some(true)
    )

    val rnaConfiguration = RNASeqConfiguration(
      analysisId = AnalysisId("default"),
      referenceFasta = referenceFasta,
      geneModelGtf = gtfFile.getAbsolutePath,
      qtlToolsCommandLineArguments = Nil,
      quantificationGtf = gtfFile.getAbsolutePath,
      starVersion = None
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
