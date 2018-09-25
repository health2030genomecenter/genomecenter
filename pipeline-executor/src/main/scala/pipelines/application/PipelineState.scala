package org.gc.pipelines.application

import scala.concurrent.Future

trait PipelineState {
  def processingFinished(r: RunfolderReadyForProcessing): Future[Unit]
  def incompleteRuns: Future[List[RunfolderReadyForProcessing]]
  def completed(r: RunfolderReadyForProcessing): Future[Boolean]
  def registerNewRun(r: RunfolderReadyForProcessing): Future[Unit]
}

class InMemoryPipelineState extends PipelineState {
  private var incomplete = List[RunfolderReadyForProcessing]()
  private var completed = List[RunfolderReadyForProcessing]()
  def incompleteRuns = Future.successful(incomplete)
  def registerNewRun(r: RunfolderReadyForProcessing) = synchronized {
    incomplete = r :: incomplete
    Future.successful(())
  }
  def processingFinished(r: RunfolderReadyForProcessing) = synchronized {
    incomplete = incomplete.filterNot(_ == r)
    completed = r :: completed
    Future.successful(())
  }
  def completed(r: RunfolderReadyForProcessing) =
    Future.successful(completed.contains(r))

}
