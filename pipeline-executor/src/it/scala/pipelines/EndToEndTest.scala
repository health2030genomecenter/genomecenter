package org.gc.pipelines

import org.scalatest._

import akka.stream.ActorMaterializer

import EndToEndHelpers._
import PipelineFixtures.{read1, read2}
import GenericTestHelpers.timeout

import scala.concurrent.duration._

import org.gc.pipelines.application.{
  SampleFinished,
  ProjectFinished,
  RunfolderReadyForProcessing,
  ProgressData,
  WESConfiguration,
  AnalysisConfiguration
}
import org.gc.pipelines.model.Project

import org.scalatest._

import tasks._

import org.gc.pipelines.model._
import org.gc.pipelines.util.StableSet

import org.gc.pipelines.util.StableSet

import org.gc.pipelines.GenericTestHelpers._
import org.gc.pipelines.PipelineFixtures._

import io.circe.syntax._

class EndToEndTestSuite extends FunSuite with Matchers with GivenWhenThen {

  test("e2e test") {
    new Fixture {

      Given("a running application")
      withApplication { implicit app =>
        implicit val AS = app.pipelinesApplication.actorSystem
        implicit val mat = ActorMaterializer()
        implicit val tsc = app.pipelinesApplication.taskSystem.components
        import AS.dispatcher
        val probe = createProbe
        And("A configuration with an already demultiplexed set of fastq files")
        val runConfiguration = s"""
            runId = runid1
            demultiplexing = []
            fastqs = [
              {
                project = project1
                sampleId = sample1
                lanes = [
                  {
                    lane = 1
                    read1 = $read1
                    read2 = $read2
                  }
                ]
              }
            ]
        """
        And("A WES analysis configuration")

        When("assigning the analysis to the project")
        postString(
          "/v2/analyses/project1",
          (wesConfiguration: AnalysisConfiguration).asJson.noSpaces).status.intValue shouldBe 200

        And("registering that runconfiguration to the application")
        postString("/v2/runs", runConfiguration).status.intValue shouldBe 200

        Then("The sample's processing should finish")
        probe.expectMsgPF(timeout) {
          case sample: SampleFinished[_] =>
            sample.sample shouldBe "sample1"
        }
        And("The project's processing should finish")
        And("The delivery list should be created")
        probe.fishForSpecificMessage(timeout) {
          case projectFinished: ProjectFinished[_] =>
            projectFinished.project shouldBe "project1"
            val deliverableListLines =
              extractDeliverableList(projectFinished, Project("project1"))

            assert(deliverableListLines.head.contains("runid1"))
            assert(
              deliverableListLines.exists(
                _.endsWith("projectQC/project1/project1.1.wes.qc.table.html")))
            assert(deliverableListLines.exists(_.endsWith(
              "projects/project1/sample1/fastp/runid1/project1.sample1.runid1.fastp.html")))
            assert(
              deliverableListLines.exists(
                _.endsWith("projectQC/project1/project1.1.readqc.pdf")))
        }

        When(
          "registering that runconfiguration to the application for a second time")
        postString("/v2/runs", runConfiguration).status.intValue shouldBe 200
        Then("The sample's processing should finish immediately")
        probe.expectMsgPF(15 seconds) {
          case sample: SampleFinished[_] =>
            sample.sample shouldBe "sample1"
        }
        getProgress("/v2/runs") shouldBe "runid1\n"
        getProgress("/v2/runs/runid1") shouldBe "demultiplex started\ndemultiplexed 1\ndemultiplex started\ndemultiplexed 1\n"
        getProgress("/v2/projects") shouldBe "project1\n"
        MyTestKit.awaitAssert(
          io.circe.parser
            .decode[Seq[ProgressData]](getProgress("/v2/projects/project1"))
            .right
            .get
            .size shouldBe 10,
          30 seconds)
        getProgress("/v2/bams/project1").size > 3 shouldBe true
        getProgress("/v2/vcfs/project1") shouldBe "\n"

        getProgress("/v2/coverages/project1") shouldBe "sample1\trunid1\tdefault\t0.005563\n"
        getProgress("/v2/fastqs/project1").size > 3 shouldBe true

        io.circe.parser
          .decode[Seq[(String, Seq[AnalysisConfiguration])]](
            getProgress("/v2/analyses"))
          .right
          .get

        io.circe.parser
          .decode[Seq[RunfolderReadyForProcessing]](
            getProgress("/v2/runconfigurations/runid1"))
          .right
          .get

        io.circe.parser
          .decode[Option[ProgressData]](getProgress("/v2/deliveries/project1"))
          .right
          .get
          .isDefined shouldBe true
        When("reprocessing all run")
        postString("/v2/reprocess", "").status.intValue shouldBe 200
        Then("The sample's processing should finish immediately")
        probe.expectMsgPF(15 seconds) {
          case sample: SampleFinished[_] =>
            sample.sample shouldBe "sample1"
        }
        And("The progress data should reflect the 3rd set of processing steps")
        MyTestKit.awaitAssert(
          io.circe.parser
            .decode[Seq[ProgressData]](getProgress("/v2/projects/project1"))
            .right
            .get
            .size shouldBe 15,
          5 seconds)

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
      doVariantCalls = Some(false),
      doJointCalls = Some(false),
      minimumTargetCoverage = None,
      minimumWGSCoverage = None,
      variantCallingContigs = None
    )
  }

}
