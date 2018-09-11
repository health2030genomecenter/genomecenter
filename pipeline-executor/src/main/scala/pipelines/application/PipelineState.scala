package org.gc.pipelines.application

import scala.concurrent.Future

trait PipelineState {
  def processingFinished(r: RunfolderReadyForProcessing): Future[Unit]
  def incompleteRuns: Future[List[RunfolderReadyForProcessing]]
  def registerNewRun(r: RunfolderReadyForProcessing): Future[Unit]
}

class InMemoryPipelineState extends PipelineState {
  private var state = List[RunfolderReadyForProcessing]()
  def incompleteRuns = Future.successful(state)
  def registerNewRun(r: RunfolderReadyForProcessing) = synchronized {
    state = r :: state
    Future.successful(())
  }
  def processingFinished(r: RunfolderReadyForProcessing) = synchronized {
    state = state.filterNot(_ == r)
    Future.successful(())
  }

}
