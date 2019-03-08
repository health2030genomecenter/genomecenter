package org.gc.pipelines

import com.typesafe.scalalogging.StrictLogging
import com.typesafe.config.{ConfigFactory}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import tasks._

object Main extends App with StrictLogging {
  logger.info("Main thread started.")

  implicit val config = {
    val fromClassPath = ConfigFactory.load
    val version = org.gc.buildinfo.BuildInfo.version
    logger.info("Version: " + version)
    val withVersion =
      ConfigFactory.parseString(s"tasks.codeVersion = $version ")
    withVersion.withFallback(fromClassPath)
  }
  logger.debug("Full config tree: \n" + config.root.render)

  implicit val taskSystem = defaultTaskSystem(Some(config))

  if (taskSystem.hostConfig.isApp) {

    implicit val actorSystem = ActorSystem("Main")
    implicit val materializer = ActorMaterializer()
    import scala.concurrent.ExecutionContext.Implicits.global
    new Application

  } else {
    logger.info("Worker started.")
  }

  logger.info("Main thread will stop.")
}
