package org.gc.pipelines

import org.gc.pipelines.application._
import com.typesafe.config.{ConfigFactory}
import java.io.File
import com.typesafe.scalalogging.StrictLogging

object PipelineConfiguration extends StrictLogging {
  val config = ConfigFactory.load.getConfig("gc.pipeline")

  val pipelineState =
    if (config.hasPath("stateLog")) {
      val file = new File(config.getString("stateLog"))
      logger.info("Saving pipeline state to " + file)
      new FilePipelineState(file)
    } else {
      logger.info("Discarding pipeline state.")
      new InMemoryPipelineState
    }

}
