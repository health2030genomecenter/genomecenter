package org.gc.pipelines

import org.scalatest._

import org.scalatest.{Matchers, BeforeAndAfterAll}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import tasks._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.typesafe.scalalogging.StrictLogging

import org.gc.pipelines.application._
import org.gc.pipelines.model._

import scala.concurrent.ExecutionContext.Implicits.global

class PipelinesApplicationTest
    extends TestKit(ActorSystem("PipelinesApplication"))
    with FunSuiteLike
    with BeforeAndAfterAll
    with GivenWhenThen
    with Matchers {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  // test("file based state logging should work") {
  //   val file = fileutils.TempFile.createTempFile("log")
  //   val pipelineState = new FilePipelineState(file)

  //   val run = RunfolderReadyForProcessing(RunId("fake"),
  //                                         "fakePath",
  //                                         RunConfiguration(false,
  //                                                          Set.empty,
  //                                                          "fake",
  //                                                          "fake",
  //                                                          Set(),
  //                                                          Selector.empty,
  //                                                          Selector.empty,
  //                                                          None,
  //                                                          "fake",
  //                                                          "fake",
  //                                                          "fake"))

  //   pipelineState.registerNewRun(run)

  //   Await.result((new FilePipelineState(file)).incompleteRuns, 5 seconds) shouldBe List(
  //     run)

  //   pipelineState.processingFinished(run)
  //   Await.result((new FilePipelineState(file)).incompleteRuns, 5 seconds) shouldBe Nil
  // }

  test(
    "pipelines application should react if a RunfolderReady event is received") {
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 3
    val eventSource = new FakeSequencingCompleteEventSource(numberOfRuns, false)
    val pipelineState = new InMemoryPipelineState

    val taskSystem = defaultTaskSystem(Some(config))

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       TestPipeline)

    val processedRuns = Await.result(app.processingFinishedSource
                                       .runWith(Sink.seq),
                                     atMost = 25 seconds)

    processedRuns
      .collect {
        case RunFinished(_, success) => success
      }
      .count(identity) shouldBe numberOfRuns

    processedRuns
      .collect {
        case SampleFinished(_, _, _, success, _) => success
      }
      .count(identity) shouldBe 4 * numberOfRuns

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
                       RunId("fake0"),
                       "fake0"),
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake1"),
                       "fake0fake1"),
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake2"),
                       "fake0fake1fake2")
    )

  }

  test(
    "pipelines application should not react if the same RunfolderReady event is received multiple times") {
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 3
    val eventSource =
      new FakeSequencingCompleteEventSource(numberOfRuns, uniform = true)
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       TestPipeline)

    val processedRuns = Await.result(app.processingFinishedSource
                                       .runWith(Sink.seq),
                                     atMost = 15 seconds)

    processedRuns
      .collect {
        case RunFinished(_, success) => success
      }
      .count(identity) shouldBe 1

  }

  test(
    "pipelines application should respect the pipeline's `canProcess` method") {
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 3
    val eventSource = new FakeSequencingCompleteEventSource(numberOfRuns, false)
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       TestPipelineWhichNeverRuns)

    val processedRuns = Await.result(app.processingFinishedSource
                                       .runWith(Sink.seq),
                                     atMost = 15 seconds)

    processedRuns
      .collect {
        case RunFinished(_, success) => success
      }
      .count(identity) shouldBe 0

  }

  test("pipelines application should survive a failing pipeline") {
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 3
    val eventSource = new FakeSequencingCompleteEventSource(numberOfRuns, false)
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       TestPipelineWhichFails)

    val processedRuns = Await.result(app.processingFinishedSource
                                       .runWith(Sink.seq),
                                     atMost = 15 seconds)

    processedRuns
      .collect {
        case RunFinished(_, success) => success
      }
      .count(identity) shouldBe 0

  }

}

class FakeSequencingCompleteEventSource(take: Int, uniform: Boolean)
    extends SequencingCompleteEventSource
    with StrictLogging {
  def commands =
    Source
      .tick(
        1 seconds,
        2 seconds,
        RunfolderReadyForProcessing(
          RunId("fake"),
          "fakePath",
          RunConfiguration(false,
                           Set.empty,
                           "fake",
                           "fake",
                           Set(),
                           Selector.empty,
                           Selector.empty,
                           None,
                           "fake",
                           "fake",
                           "fake",
                           "fake",
                           "fake",
                           "fake",
                           "fake")
        )
      )
      .take(take.toLong)
      .zipWithIndex
      .map {
        case (run, idx) =>
          if (uniform) run
          else
            run.copy(runId = RunId(run.runId + idx))
      }
      .map(Append(_))
}

case class FakeDemultiplexed(
    project: Project,
    sampleId: SampleId,
    runId: RunId
)

case class FakeSampleResult(
    project: Project,
    sampleId: SampleId,
    runId: RunId,
    accumulator: String
)

trait FakePipeline extends Pipeline[FakeDemultiplexed, FakeSampleResult] {

  def demultiplex(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents): Future[Seq[FakeDemultiplexed]] =
    Future.successful(
      List(
        FakeDemultiplexed(Project("project1"), SampleId("sample1"), r.runId),
        FakeDemultiplexed(Project("project1"), SampleId("sample2"), r.runId),
        FakeDemultiplexed(Project("project2"), SampleId("sample1"), r.runId),
        FakeDemultiplexed(Project("project2"), SampleId("sample2"), r.runId)
      )
    )

  def getKeysOfDemultiplexedSample(
      d: FakeDemultiplexed): (Project, SampleId, RunId) =
    (d.project, d.sampleId, d.runId)
  def getKeysOfSampleResult(d: FakeSampleResult): (Project, SampleId, RunId) =
    (d.project, d.sampleId, d.runId)

  def processCompletedRun(samples: Seq[FakeSampleResult])(
      implicit tsc: TaskSystemComponents): Future[(RunId, Boolean)] =
    Future.successful {
      samples.head.runId -> true
    }

  def processCompletedProject(samples: Seq[FakeSampleResult])(
      implicit tsc: TaskSystemComponents): Future[(Project, Boolean)] =
    Future.successful {
      samples.head.project -> true
    }

  def processSample(runConfiguration: RunfolderReadyForProcessing,
                    pastSampleResult: Option[FakeSampleResult],
                    demultiplexedSample: FakeDemultiplexed)(
      implicit tsc: TaskSystemComponents): Future[Option[FakeSampleResult]] =
    Future.successful {
      Some(
        FakeSampleResult(
          demultiplexedSample.project,
          demultiplexedSample.sampleId,
          demultiplexedSample.runId,
          pastSampleResult
            .map(_.accumulator)
            .getOrElse("") + demultiplexedSample.runId
        ))
    }

  def canProcess(r: RunfolderReadyForProcessing) = true
}

object TestPipeline extends FakePipeline

object TestPipelineWhichNeverRuns extends FakePipeline {

  override def canProcess(r: RunfolderReadyForProcessing) = false

}

object TestPipelineWhichFails extends FakePipeline {

  override def canProcess(r: RunfolderReadyForProcessing) = true

  def fail = {
    def zero = 0
    1 / zero
  }

  override def processSample(runConfiguration: RunfolderReadyForProcessing,
                             pastSampleResult: Option[FakeSampleResult],
                             demultiplexedSample: FakeDemultiplexed)(
      implicit tsc: TaskSystemComponents): Future[Option[FakeSampleResult]] =
    Future.successful {
      fail
      None
    }

}
