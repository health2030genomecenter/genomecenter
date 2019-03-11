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
  val port = 9099
}

class Application(implicit ec: ExecutionContext,
                  actorSystem: ActorSystem,
                  mat: Materializer,
                  taskSystem: TaskSystem,
                  config: Config)
    extends StrictLogging {

  val commandSource = new HttpCommandSource
  val progressServer = new ProgressServer
  val pipelineState = PipelineConfiguration.pipelineState
  val queryComponent = new ConfigurationQueryHttpComponent(pipelineState)

  val httpServer =
    new HttpServer(
      port = MainConfig.port,
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
