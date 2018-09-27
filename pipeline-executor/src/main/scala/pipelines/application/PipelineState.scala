package org.gc.pipelines.application

import scala.concurrent.Future
import com.typesafe.scalalogging.StrictLogging

trait PipelineState {
  def processingFinished(r: RunfolderReadyForProcessing): Future[Unit]
  def incompleteRuns: Future[List[RunfolderReadyForProcessing]]
  def completed(r: RunfolderReadyForProcessing): Future[Boolean]
  def registerNewRun(r: RunfolderReadyForProcessing): Future[Unit]
}

class InMemoryPipelineState extends PipelineState with StrictLogging {
  private var incomplete = List[RunfolderReadyForProcessing]()
  private var completed = List[RunfolderReadyForProcessing]()
  def incompleteRuns = {
    logger.debug(s"Querying incomplete runs (${incomplete.size})")
    Future.successful(incomplete)
  }
  def registerNewRun(r: RunfolderReadyForProcessing) = synchronized {
    logger.info(s"Registering run ${r.runId}")
    incomplete = r :: incomplete
    Future.successful(())
  }
  def processingFinished(r: RunfolderReadyForProcessing) = synchronized {
    logger.info(s"Saving finished run ${r.runId}")
    incomplete = incomplete.filterNot(_ == r)
    completed = r :: completed
    Future.successful(())
  }
  def completed(r: RunfolderReadyForProcessing) = {
    logger.debug(s"Querying run's ${r.runId} completion")
    Future.successful(completed.contains(r))
  }

}
