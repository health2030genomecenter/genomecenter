package org.gc.pipelines.application

import scala.concurrent.Future
import com.typesafe.scalalogging.StrictLogging
import org.gc.pipelines.model.{RunId, Project, AnalysisId}

case class RunWithAnalyses(run: RunfolderReadyForProcessing,
                           analyses: AnalysisAssignments) {
  def runId = run.runId
}
trait PipelineState {

  def pastRuns: Future[List[RunWithAnalyses]]
  def registered(
      r: RunfolderReadyForProcessing): Future[Option[RunWithAnalyses]]
  def invalidated(runId: RunId): Future[Unit]
  def contains(r: RunId): Future[Boolean]

  def assigned(project: Project,
               analysisConfiguration: AnalysisConfiguration): Future[Unit]
  def unassigned(project: Project, analysisId: AnalysisId): Future[Unit]

}

class InMemoryPipelineState extends PipelineState with StrictLogging {
  private var past = List[RunfolderReadyForProcessing]()
  def contains(r: RunId) = {
    Future.successful(past.exists(_.runId == r))
  }
  def pastRuns = {
    logger.debug(s"Querying incomplete runs (${past.size})")
    Future.successful(past.map(r => RunWithAnalyses(r, analyses)))
  }
  def registered(r: RunfolderReadyForProcessing) = synchronized {
    logger.info(s"Registering run ${r.runId}")
    past = (past.filterNot(_.runId == r.runId)) :+ r
    Future.successful(Some(RunWithAnalyses(r, analyses)))
  }
  def invalidated(runId: RunId) = synchronized {
    Future.successful(())
  }

  private var analyses = AnalysisAssignments.empty

  def assigned(project: Project,
               analysisConfiguration: AnalysisConfiguration): Future[Unit] = {
    logger.info(s"Assigning $project to $analysisConfiguration")
    synchronized {
      analyses = analyses.assigned(project, analysisConfiguration)
    }
    Future.successful(())
  }

  def unassigned(project: Project, analysisId: AnalysisId): Future[Unit] = {
    logger.info(s"Assigning $project to $analysisId")
    synchronized { analyses = analyses.unassigned(project, analysisId) }
    Future.successful(())
  }

}
