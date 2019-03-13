package org.gc.pipelines.application

import scala.concurrent.Future
import com.typesafe.scalalogging.StrictLogging
import org.gc.pipelines.model.{RunId, Project, AnalysisId}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Encoder, Decoder}

case class RunWithAnalyses(run: RunfolderReadyForProcessing,
                           analyses: AnalysisAssignments) {
  def runId = run.runId
}
/* Persisted state of the application
 *
 * Methods with names in perfect form and taking an argument are processing events.
 * They might or might not be idempotent.
 *
 * Other methods query the state.
 */
trait PipelineState {

  /* Query all past runs joined with the most recent analysis configuration */
  def pastRuns: Future[List[RunWithAnalyses]]

  /* Process registered event
   *
   * After this event has been processed all future calls to
   * `pastRuns` should include this run
   */
  def registered(
      r: RunfolderReadyForProcessing): Future[Option[RunWithAnalyses]]

  /* Process invalidated event
   *
   * Future calls to `pastRuns` should not contain any run with this runId
   */
  def invalidated(runId: RunId): Future[Unit]

  /* Query */
  def contains(r: RunId): Future[Boolean]

  /* Process assigned event
   *
   * Future calls to `pastRuns` and `analyses` should reflect this (project, analysisConfiguration)
   * pair
   */
  def assigned(project: Project,
               analysisConfiguration: AnalysisConfiguration): Future[Unit]

  /* Process unassigned event
   *
   * Future calls to `analyses` or `pastRuns` should reflect the deletion of the assignment
   */
  def unassigned(project: Project, analysisId: AnalysisId): Future[Unit]

  /* Query the current/most recent analysis configurations */
  def analyses: Future[AnalysisAssignments]

}

class InMemoryPipelineState extends PipelineState with StrictLogging {
  private var past = Vector[RunfolderReadyForProcessing]()
  private var runOrder = Vector[RunId]()
  def contains(r: RunId) = {
    Future.successful(past.exists(_.runId == r))
  }
  def pastRuns = {
    logger.debug(s"Querying incomplete runs (${past.size})")
    Future.successful(past.map(r => RunWithAnalyses(r, _analyses)).toList)
  }
  def registered(r: RunfolderReadyForProcessing) = synchronized {
    logger.info(s"Registering run ${r.runId}")
    if (!runOrder.contains(r.runId)) {
      past = past :+ r
      runOrder = runOrder :+ r.runId
    } else {
      past = (past.filterNot(_.runId == r.runId) :+ r).sortBy(r =>
        runOrder.indexOf(r.runId))
    }
    Future.successful(Some(RunWithAnalyses(r, _analyses)))
  }
  def invalidated(runId: RunId) = synchronized {
    Future.successful(())
  }

  private var _analyses = AnalysisAssignments.empty

  def analyses = Future.successful(_analyses)

  def assigned(project: Project,
               analysisConfiguration: AnalysisConfiguration): Future[Unit] = {
    logger.info(s"Assigning $project to $analysisConfiguration")
    synchronized {
      _analyses = _analyses.assigned(project, analysisConfiguration)
    }
    Future.successful(())
  }

  def unassigned(project: Project, analysisId: AnalysisId): Future[Unit] = {
    logger.info(s"Assigning $project to $analysisId")
    synchronized { _analyses = _analyses.unassigned(project, analysisId) }
    Future.successful(())
  }

}

object RunWithAnalyses {
  implicit val encoder: Encoder[RunWithAnalyses] =
    deriveEncoder[RunWithAnalyses]
  implicit val decoder: Decoder[RunWithAnalyses] =
    deriveDecoder[RunWithAnalyses]
}
