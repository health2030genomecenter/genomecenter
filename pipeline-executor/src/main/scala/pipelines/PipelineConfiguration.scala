package org.gc.pipelines

import org.gc.pipelines.application._
import com.typesafe.config.{Config, ConfigFactory}
import java.io.File
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.StrictLogging

object PipelineConfiguration extends StrictLogging {
  val config = ConfigFactory.load.getConfig("gc.pipeline")

  private def parseFolderWatcher(config: Config) = {
    val path = config.getString("path")
    val lastFile = config.getString("last")
    val sampleSheetFolder = config.getString("sampleSheetFolder")
    FolderWatcherEventSource(path, lastFile, new File(sampleSheetFolder))
  }

  val folderWatchers =
    config.getConfigList("folders").asScala.map(parseFolderWatcher)

  val eventSource = folderWatchers match {
    case Seq(first, second, rest @ _*) =>
      CompositeSequencingCompleteEventSource(first, second, rest: _*)
    case Seq(first) => first
    case _          => EmptySequencingCompleteEventSource
  }

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
