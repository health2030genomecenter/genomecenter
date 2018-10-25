package org.gc.pipelines

import org.scalatest._

import org.scalatest.{Matchers, BeforeAndAfterAll}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import tasks._
import tasks.circesupport._

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
                                       List(TestPipeline))

    val processedRuns = Await.result(app.processingFinishedSource
                                       .runWith(Sink.seq),
                                     atMost = 25 seconds)

    processedRuns.count(_.success) shouldBe numberOfRuns

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
                                       List(TestPipeline))

    val processedRuns = Await.result(app.processingFinishedSource
                                       .runWith(Sink.seq),
                                     atMost = 15 seconds)

    processedRuns.count(_.success) shouldBe 1

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
                                       List(TestPipelineWhichNeverRuns))

    val processedRuns = Await.result(app.processingFinishedSource
                                       .runWith(Sink.seq),
                                     atMost = 15 seconds)

    processedRuns.count(_.success) shouldBe 0

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
                                       List(TestPipelineWhichFails))

    val processedRuns = Await.result(app.processingFinishedSource
                                       .runWith(Sink.seq),
                                     atMost = 15 seconds)

    processedRuns.count(_.success) shouldBe 0

  }

}

class FakeSequencingCompleteEventSource(take: Int, uniform: Boolean)
    extends SequencingCompleteEventSource
    with StrictLogging {
  def events =
    Source
      .tick(
        1 seconds,
        2 seconds,
        RunfolderReadyForProcessing("fake",
                                    "fakePath",
                                    RunConfiguration(ProcessingId("all"),
                                                     false,
                                                     "fake",
                                                     "fake",
                                                     "fake",
                                                     Set(),
                                                     Nil,
                                                     (1, 2),
                                                     None,
                                                     Selector.empty,
                                                     Selector.empty,
                                                     None,
                                                     "fake"))
      )
      .take(take.toLong)
      .zipWithIndex
      .map {
        case (run, idx) =>
          if (uniform) run
          else
            run.copy(runId = run.runId + idx)
      }
}

object TestPipeline extends Pipeline {

  def canProcess(r: RunfolderReadyForProcessing) = true

  val pretend =
    AsyncTask[RunfolderReadyForProcessing, Int]("demultiplexing", 1) {
      input => implicit computationEnvironment =>
        log.info(s"Pretending that run $input is being processed..")
        Future.successful(1)
    }

  def execute(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents): Future[Boolean] =
    pretend(r)(ResourceRequest(1, 500)).map(_ => true)

}

object TestPipelineWhichNeverRuns extends Pipeline {

  def canProcess(r: RunfolderReadyForProcessing) = false

  val pretend =
    AsyncTask[RunfolderReadyForProcessing, Int]("demultiplexing-never-run", 1) {
      input => implicit computationEnvironment =>
        log.info(s"Pretending that run $input is being processed..")
        Future.successful(1)
    }

  def execute(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents): Future[Boolean] =
    pretend(r)(ResourceRequest(1, 500)).map(_ => true)

}

object TestPipelineWhichFails extends Pipeline {

  def canProcess(r: RunfolderReadyForProcessing) = true

  def fail = {
    def zero = 0
    1 / zero
  }

  val pretend =
    AsyncTask[RunfolderReadyForProcessing, Int]("demultiplexing-failure", 1) {
      input => implicit computationEnvironment =>
        log.info(s"Pretending that run $input is being processed and failed..")
        Future { fail }(computationEnvironment.executionContext)
    }

  def execute(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents): Future[Boolean] = {
    pretend(r)(ResourceRequest(1, 500)).map(_ => true)
  }

}
