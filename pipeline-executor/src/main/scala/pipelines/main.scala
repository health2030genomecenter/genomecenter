package org.gc.pipelines

import com.typesafe.scalalogging.StrictLogging
import com.typesafe.config.{ConfigFactory}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import tasks._

/** Main entrypoint of the application process (main method)
  *
  * It is not expected that the application stops on its own
  * Shutting down the JVM via a signal shuts down gracefully the application
  *
  * During graceful shutdown worker nodes/jobs are stopped, files are saved and
  * file handles are closed, temporary scratch files are deleted.
  *
  * See the JVM's documentation on shutdown hooks on when this happens
  * (e.g. on SIGTERM and SIGINT, but not on SIGKILL)
  */
object Main extends App with StrictLogging {
  logger.info("Main thread started.")

  /* Load configuration and augment it with the buildinfo version
   *
   * Important: understand where the config is loaded as described in
   * https://github.com/lightbend/config#standard-behavior
   */
  implicit val config = {
    val fromClassPath = ConfigFactory.load
    val version = org.gc.buildinfo.BuildInfo.version
    logger.info("Version: " + version)
    val withVersion =
      ConfigFactory.parseString(s"tasks.codeVersion = $version ")
    withVersion.withFallback(fromClassPath)
  }
  logger.debug("Full config tree: \n" + config.root.render)

  /* Create and intialize a TaskSystem instance provided by the tasks library

   * This creates its own ActorSystem as well
   * This instance is holds data structures responsible for:
   * - spawning worker jobs
   * - resolving and creating SharedFile instances
   *
   * It has shutdown hooks to shut itself down on JVM termination
   */
  implicit val taskSystem = defaultTaskSystem(Some(config))

  /* Main branching point between worker and master mode */
  if (taskSystem.hostConfig.isApp) {

    /* This is an ActorSystem used by the application for its own purposes
     * i.e. to run the streams
     */
    implicit val actorSystem = ActorSystem("Main")
    implicit val materializer = ActorMaterializer()
    import scala.concurrent.ExecutionContext.Implicits.global
    new Application

  } else {
    logger.info("Worker started.")
  }

  // JVM shuts down when the last non-daemon thread stops
  // Both the above `actorSystem` and the actor system in `taskSystem`
  // spawned new threads, thus at this point the JVM will not exit,
  // but the application keeps running.
  logger.info("Main thread will stop.")
}
