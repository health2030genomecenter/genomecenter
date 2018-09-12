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

  ignore(
    "pipelines application should react if a RunfolderReady event is received") {
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 3
    val eventSource = new FakeSequencingCompleteEventSource(numberOfRuns)
    val pipelineState = new InMemoryPipelineState

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       config,
                                       implicitly[ActorSystem],
                                       List(TestPipeline))

    val processedRuns = Await.result(app.processingFinishedSource
                                       .runWith(Sink.seq),
                                     atMost = 15 seconds)

    processedRuns.count(_.success) shouldBe numberOfRuns

  }

  ignore(
    "pipelines application should respect the pipeline's `canProcess` method") {
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 3
    val eventSource = new FakeSequencingCompleteEventSource(numberOfRuns)
    val pipelineState = new InMemoryPipelineState

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       config,
                                       implicitly[ActorSystem],
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
    val eventSource = new FakeSequencingCompleteEventSource(numberOfRuns)
    val pipelineState = new InMemoryPipelineState

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       config,
                                       implicitly[ActorSystem],
                                       List(TestPipelineWhichFails))

    val processedRuns = Await.result(app.processingFinishedSource
                                       .runWith(Sink.seq),
                                     atMost = 15 seconds)

    processedRuns.count(_.success) shouldBe 0

  }

}

class FakeSequencingCompleteEventSource(take: Int)
    extends SequencingCompleteEventSource
    with StrictLogging {
  def events =
    Source
      .tick(
        1 seconds,
        2 seconds,
        RunfolderReadyForProcessing("fake", SampleSheet("fake"), "fakePath"))
      .take(take.toLong)
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
    pretend(r)(CPUMemoryRequest(1, 500)).map(_ => true)

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
    pretend(r)(CPUMemoryRequest(1, 500)).map(_ => true)

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
    pretend(r)(CPUMemoryRequest(1, 500)).map(_ => true)
  }

}
