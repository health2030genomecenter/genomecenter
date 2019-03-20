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
  RunfolderReadyForProcessing
}
import org.gc.pipelines.model.Project

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
        When("registering that runconfiguration to the application")
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
            assert(
              deliverableListLines.exists(
                _.endsWith("premade_fastqs/project1/papa.read1.fq.gz")))
            assert(
              deliverableListLines.exists(
                _.endsWith("premade_fastqs/project1/papa.read2.fq.gz")))
        }

        When(
          "registering that runconfiguration to the application for a second time")
        postString("/v2/runs", runConfiguration).status.intValue shouldBe 200
        Then("The sample's processing should finish immediately")
        probe.expectMsgPF(15 seconds) {
          case sample: SampleFinished[_] =>
            sample.sample shouldBe "sample1"
        }
        getProgress("/v2/runs") shouldBe "runid1"
        getProgress("/v2/runs/runid1") shouldBe "demultiplex started\ndemultiplexed 1\ndemultiplex started\ndemultiplexed 1"
        getProgress("/v2/projects") shouldBe "project1"
        getProgress("/v2/projects/project1") shouldBe """[{"DemultiplexedSample":{"project":"project1","sampleId":"sample1","run":"runid1"}},{"SampleProcessingStarted":{"project":"project1","sample":"sample1","runId":"runid1"}},{"SampleProcessingFinished":{"project":"project1","sample":"sample1","run":"runid1"}},{"DemultiplexedSample":{"project":"project1","sampleId":"sample1","run":"runid1"}},{"SampleProcessingStarted":{"project":"project1","sample":"sample1","runId":"runid1"}},{"SampleProcessingFinished":{"project":"project1","sample":"sample1","run":"runid1"}}]"""
        getProgress("/v2/bams/project1") shouldBe ""
        getProgress("/v2/vcfs/project1") shouldBe ""
        getProgress("/v2/analyses") shouldBe "[]"
        io.circe.parser
          .decode[Seq[RunfolderReadyForProcessing]](
            getProgress("/v2/runconfigurations/runid1"))
          .right
          .get

      }
    }

  }

  trait Fixture {}

}
