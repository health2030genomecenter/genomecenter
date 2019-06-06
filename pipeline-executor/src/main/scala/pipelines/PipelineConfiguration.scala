package org.gc.pipelines

import org.gc.pipelines.application._
import com.typesafe.config.{ConfigFactory}
import java.io.File
import com.typesafe.scalalogging.StrictLogging
import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext

object PipelineConfiguration extends StrictLogging {
  val config = ConfigFactory.load.getConfig("gc.pipeline")

  def pipelineState(implicit AS: ActorSystem, ec: ExecutionContext) =
    if (config.hasPath("stateLog")) {
      val file = new File(config.getString("stateLog"))
      logger.info("Saving pipeline state to " + file)
      new FilePipelineState(file)
    } else {
      logger.info("Discarding pipeline state.")
      new InMemoryPipelineState
    }

}
