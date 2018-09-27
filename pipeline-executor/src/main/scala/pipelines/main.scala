package org.gc.pipelines

import com.typesafe.scalalogging.StrictLogging
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import tasks._

import org.gc.pipelines.application._
import org.gc.pipelines.stages.ProtoPipeline

object Main extends App with StrictLogging {
  logger.info("Main thread started.")

  val config = ConfigFactory.load

  private val taskSystem = defaultTaskSystem(Some(config))

  if (taskSystem.hostConfig.isApp) {

    implicit val actorSystem = ActorSystem("Main")
    implicit val materializer = ActorMaterializer()
    import scala.concurrent.ExecutionContext.Implicits.global
    val eventSource =
      CompositeSequencingCompleteEventSource(PipelineConfiguration.eventSource,
                                             new HttpServer)

    val pipelineState = PipelineConfiguration.pipelineState

    val pipeline =
      new ProtoPipeline

    new PipelinesApplication(eventSource,
                             pipelineState,
                             actorSystem,
                             taskSystem,
                             List(pipeline))
  } else {
    logger.info("Worker started.")
  }

  logger.info("Main thread will stop.")
}
