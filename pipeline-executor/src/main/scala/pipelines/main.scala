package org.gc.pipelines

import com.typesafe.scalalogging.StrictLogging
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import tasks._

import org.gc.pipelines.application._
import org.gc.pipelines.model.{Project, SampleId}
import org.gc.pipelines.stages.ProtoPipeline

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.JavaConverters._

object Main extends App with StrictLogging {
  logger.info("Main thread started.")

  val config = {
    val fromClassPath = ConfigFactory.load
    val version = org.gc.buildinfo.BuildInfo.version
    logger.info("Version: " + version)
    val withVersion =
      ConfigFactory.parseString(s"tasks.codeVersion = $version ")
    withVersion.withFallback(fromClassPath)
  }
  logger.debug("Full config tree: \n" + config.root.render)

  private val taskSystem = defaultTaskSystem(Some(config))

  if (taskSystem.hostConfig.isApp) {

    implicit val actorSystem = ActorSystem("Main")
    implicit val materializer = ActorMaterializer()
    import scala.concurrent.ExecutionContext.Implicits.global
    val eventSource = new HttpServer(port = 9099)
    val httpBinding = eventSource.startServer
    httpBinding.andThen {
      case scala.util.Success(serverBinding) =>
        logger.info(
          s"Pipeline application's web server is listening on ${serverBinding.localAddress}")
    }(actorSystem.dispatcher)

    val pipelineState = PipelineConfiguration.pipelineState

    val pipeline =
      new ProtoPipeline

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

      new PersistEventSource(eventSource, pipelineState)

      new SimplePipelinesApplication(pastRuns,
                                     actorSystem,
                                     taskSystem,
                                     pipeline,
                                     blacklist)
    } else
      new PipelinesApplication(eventSource,
                               pipelineState,
                               actorSystem,
                               taskSystem,
                               pipeline,
                               blacklist)
  } else {
    logger.info("Worker started.")
  }

  logger.info("Main thread will stop.")
}
