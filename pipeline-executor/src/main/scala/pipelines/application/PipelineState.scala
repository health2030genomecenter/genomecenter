package org.gc.pipelines.application

import scala.concurrent.Future
import com.typesafe.scalalogging.StrictLogging
import org.gc.pipelines.model.RunId

trait PipelineState {
  def pastRuns: Future[List[RunfolderReadyForProcessing]]
  def registered(r: RunfolderReadyForProcessing)
    : Future[Option[RunfolderReadyForProcessing]]
  def invalidated(runId: RunId): Future[Unit]
  def contains(r: RunId): Future[Boolean]
}

class InMemoryPipelineState extends PipelineState with StrictLogging {
  private var past = List[RunfolderReadyForProcessing]()
  def contains(r: RunId) = {
    Future.successful(past.exists(_.runId == r))
  }
  def pastRuns = {
    logger.debug(s"Querying incomplete runs (${past.size})")
    Future.successful(past)
  }
  def registered(r: RunfolderReadyForProcessing) = synchronized {
    logger.info(s"Registering run ${r.runId}")
    past = r :: past
    Future.successful(Some(r))
  }
  def invalidated(runId: RunId) = synchronized {
    Future.successful(())
  }

}
