package org.gc.pipelines.application

import com.typesafe.scalalogging.StrictLogging
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem

object Main extends App with StrictLogging {
  logger.info("Main thread started.")
  val config = ConfigFactory.load

  val eventSource: SequencingCompleteEventSource =
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
  val app =
    new PipelinesApplication(eventSource, pipelineState, config, actorSystem)
  logger.info("Main thread will stop.")
}
