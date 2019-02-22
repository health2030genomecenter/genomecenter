package org.gc.pipelines

import org.scalatest._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import tasks._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.ActorMaterializer

import org.gc.pipelines.application._
import org.gc.pipelines.model._
import org.gc.pipelines.util.StableSet

import scala.concurrent.ExecutionContext.Implicits.global

class SimplePipelinesApplicationTest
    extends FunSuite
    with GivenWhenThen
    with Matchers {

  test("pipelines application should replace old runs - pattern 1 2 3 2") {
    implicit val AS = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 4
    Given(
      "A sequence of three runs with order 1 2 3 2, i.e. is the middle run is repeated")
    val eventSource =
      new FakeSequencingCompleteEvents(numberOfRuns,
                                       uniform = false,
                                       pattern = List(1, 2, 3, 2))
    val pipelineState = new InMemoryPipelineState
    Await.result(
      Future.traverse(eventSource.commands)(r => pipelineState.registered(r)),
      5 seconds)
    val taskSystem = defaultTaskSystem(Some(config))

    When("Sending these run sequence into a running application")
    val app = new SimplePipelinesApplication(
      Await.result(pipelineState.pastRuns, atMost = 5 seconds),
      implicitly[ActorSystem],
      taskSystem,
      new TestPipeline)

    val processedRuns =
      Await.result(
        app.processingFinishedSource
          .scan(Seq.empty[AnyRef])((seq, t) => seq :+ t)
          .takeWhile(samplesFinished(
                       FakeSampleResult(Project("project1"),
                                        SampleId("sample1"),
                                        RunId("fake2"),
                                        "fake1_0fake3_0fake2_0")),
                     true)
          .runWith(Sink.last),
        atMost = 60 seconds
      )

    Then(
      "Each run should be processed by taking the previous into account and the when the middle run is sent the second time the third should be reprocessed but not the first")
    processedRuns
      .collect {
        case SampleFinished(_, _, _, _, result) => result
      }
      .toSet
      .filter(_.isDefined)
      .map(_.get.asInstanceOf[FakeSampleResult])
      .filter(_.project == "project1")
      .filter(_.sampleId == "sample1")
      .toSet shouldBe Set(
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake1"),
                       "fake1_0"),
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake3"),
                       "fake1_0fake3_0"),
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake2"),
                       "fake1_0fake3_0fake2_0")
    )
    taskSystem.shutdown

  }

  def samplesFinished(waitFor: FakeSampleResult)(s: Seq[Any]): Boolean = {
    println("Sample finished: " + s.mkString("\n"))
    val r =
      (s find {
        case SampleFinished(_, _, _, _, Some(fsr)) if fsr == waitFor => true
        case _                                                       => false
      }).isEmpty
    r
  }

}

class FakeSequencingCompleteEvents(take: Int,
                                   uniform: Boolean,
                                   pattern: List[Int] = Nil) {

  val runFolder = fileutils.TempFile.createTempFile(".temp")
  runFolder.delete
  runFolder.mkdirs
  val runInfo = new java.io.File(runFolder, "RunInfo.xml")
  new java.io.FileOutputStream(runInfo).close
  val fake = fileutils.TempFile.createTempFile(".temp").getAbsolutePath

  def commands =
    (0 until take)
      .map { _ =>
        RunfolderReadyForProcessing(
          RunId("fake"),
          Some(runFolder.getAbsolutePath),
          None,
          RunConfiguration(StableSet.empty,
                           None,
                           StableSet.empty,
                           StableSet.empty)
        )
      }
      .zipWithIndex
      .map {
        case (run, idx) =>
          if (uniform) run
          else if (pattern.isEmpty)
            run.copy(runId = RunId(run.runId + idx))
          else run.copy(runId = RunId(run.runId + pattern(idx.toInt)))
      }
}
