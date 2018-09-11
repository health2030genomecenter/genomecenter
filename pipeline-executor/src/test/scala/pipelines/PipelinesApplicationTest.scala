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
    with Matchers {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  test("pipelines application should react if a run is ready") {
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

    processedRuns.size shouldBe numberOfRuns

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
