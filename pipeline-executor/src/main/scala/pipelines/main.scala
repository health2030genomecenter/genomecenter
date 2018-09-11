package org.gc.pipelines

import com.typesafe.scalalogging.StrictLogging
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import tasks._

import org.gc.pipelines.application._

object Main extends App with StrictLogging {
  logger.info("Main thread started.")

  val config = ConfigFactory.load

  val eventSource =
    CompositeSequencingCompleteEventSource(
      FolderWatcherEventSource("/data/UHTS/raw/instrument1/",
                               "SequencingComplete.txt",
                               "samplesheet.txt"),
      FolderWatcherEventSource("/data/UHTS/raw/instrument2/",
                               "SequencingComplete.txt",
                               "samplesheet.txt")
    )

  val pipelineState = new InMemoryPipelineState

  val actorSystem = ActorSystem("Main")

  // val pipeline = new Pipeline {
  //   def execute(r: RunfolderReadyForProcessing)(
  //       implicit tsc: TaskSystemComponents): Future[Unit] =
  //     Future.successful(())
  // }

  import scala.concurrent.ExecutionContext.Implicits.global
  val app =
    new PipelinesApplication(eventSource,
                             pipelineState,
                             config,
                             actorSystem,
                             Nil)

  logger.info("Main thread will stop.")
}
