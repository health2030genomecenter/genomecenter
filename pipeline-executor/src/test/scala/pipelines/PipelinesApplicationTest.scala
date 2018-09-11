package org.gc.pipelines

import org.scalatest._

import org.scalatest.{Matchers, BeforeAndAfterAll}

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.ActorMaterializer
import akka.testkit.TestKit

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
    val eventSource = new FakeSequencingCompleteEventSource
    val pipelineState = new InMemoryPipelineState

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       config,
                                       implicitly[ActorSystem])

    val processedRuns = Await.result(app.processingFinishedSource
                                       .runWith(Sink.seq),
                                     atMost = 15 seconds)

    processedRuns.size shouldBe 3

  }

}
