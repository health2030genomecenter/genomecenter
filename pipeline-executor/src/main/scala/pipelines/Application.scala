package org.gc.pipelines

import com.typesafe.scalalogging.StrictLogging
import com.typesafe.config.Config
import akka.actor.ActorSystem
import akka.stream.Materializer
import tasks._

import org.gc.pipelines.application._
import org.gc.pipelines.model.{Project, SampleId}
import org.gc.pipelines.stages.ProtoPipeline

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.collection.JavaConverters._

object MainConfig {
  val httpPort = 9099
}

/* Entry point of the application
 *
 * Isolated from the main method to ease end to end testing.
 * It takes generic dependencies (actorsystem, config, etc).
 * It instantiates the components of the application.
 * Not expected to get stopped.
 *
 * # Components and architecture
 *
 * The central piece of the application is the PipelinesApplication class.
 * It listens to commands (supplied via a CommandSource) and according to the
 * current state of the application (in PipelineState) it initiates the
 * demultiplexing of runs, processing of individual samples and whole projects.
 * The definitions of the above mentioned processing steps are described in a Pipeline object.
 *
 * ## Components:
 * - HttpCommandSource: receives http messages and translates them to commands
 * - PipelineState: receives commands, validates them against the current state, persists events, serves queries about the current state.
 * - PipelinesApplication: describe above
 * - Pipeline: a collection of recipees of how to demultiplex a run and turn a demultiplexed sample into a processed sample
 * - ProgressServer: a side effect which listens to progress updates. Its query side is connected to the http server and is used by the control cli. It is not used by the application otherwise, and must not be used (due to its side effecting nature)
 * - ConfigurationQueryHttpComponent: serves some parts of PipelineState over http for the CLI
 *
 * ## Simple Application mode
 * The process can be started in a non-daemon, non-long lived mode which executes all the processing steps defined at the moment of its startup.
 * In this mode it is still receiving, validating and persisting commands, but it does not react to them.
 * The utility of this mode is that it is much simpler than the long lived mode,
 * thus in case of emergency, it is easier to debug (and fix).
 * As of now I never used it, but prepared it for PHRT.
 *
 * ## Data flow
 * The application ingests a stream of commands (e.g. add run, assign analysis to a project) and
 * starts batch processing based on the sequence of commands.
 * The handling of transactional data (i.e. commands) follows an event sourcing pattern, while the
 * handling of data flow through the batch processing pipeline is implemented separately.
 * In particular all batch transformations are treated as memoized pure functions,
 * with memoization data written to durable storage.
 *
 * Commands are validated, converted to events and the events are both persisted into an append only event log, and folded into the in memory representation of the application state.
 * Upon startup the event log is read from the very beginning and replayed. As a consequence all
 * batch processing is also replayed, but potentially served from the memoization cache.
 *
 * The advantage of this architecture is that the application automatically handles executing
 * all outstanding work items and dependencies, and always reaches a constistent state.
 * The advantage of event sourcing is its simplicity both conceptually and in implementation (no mutable state, no transactions, no diff generation).
 * The disadvantage is that upon startup the application takes some time to replay the past, and
 * greater care has to be taken not to unintentionally invalidate the memoization caches
 * (which would trigger unintended reprocessing of past data).
 */
class Application(implicit ec: ExecutionContext,
                  actorSystem: ActorSystem,
                  mat: Materializer,
                  taskSystem: TaskSystem,
                  config: Config)
    extends StrictLogging {

  val commandSource = new HttpCommandSource
  val progressServer = new ProgressServer(taskSystem.components.actorsystem)
  val pipelineState = PipelineConfiguration.pipelineState
  val queryComponent = new ConfigurationQueryHttpComponent(pipelineState)

  val httpServer =
    new HttpServer(
      port = MainConfig.httpPort,
      Seq(commandSource.route, progressServer.route, queryComponent.route))

  val httpBinding = httpServer.startServer
  httpBinding.andThen {
    case scala.util.Success(serverBinding) =>
      logger.info(
        s"Pipeline application's web server is listening on ${serverBinding.localAddress}")
  }(actorSystem.dispatcher)

  val pipeline =
    new ProtoPipeline(progressServer)

  val useSimpleApplication = config.getBoolean("simpleApp")

  /* (project,sample) pairs on the blacklist will be ignored after demultiplexing */
  val blacklist =
    config
      .getStringList("blacklist")
      .asScala
      .grouped(2)
      .toList
      .map { list =>
        (Project(list(0)), SampleId(list(1)))
      }
      .toSet

  logger.info(s"Black list: $blacklist")

  if (useSimpleApplication) {
    val pastRuns = Await.result(pipelineState.pastRuns, atMost = 15 seconds)

    new PersistCommandSource(commandSource, pipelineState)

    new SimplePipelinesApplication(pastRuns,
                                   actorSystem,
                                   taskSystem,
                                   pipeline,
                                   blacklist)
  } else
    new PipelinesApplication(commandSource,
                             pipelineState,
                             actorSystem,
                             taskSystem,
                             pipeline,
                             blacklist)

}
