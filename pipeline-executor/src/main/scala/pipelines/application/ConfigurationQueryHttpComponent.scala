package org.gc.pipelines.application

import akka.http.scaladsl.server.Directives._
import scala.concurrent.ExecutionContext
import org.gc.pipelines.model._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

class ConfigurationQueryHttpComponent(state: PipelineState)(
    implicit ec: ExecutionContext)
    extends HttpComponent {

  def byRun(r: RunId) =
    state.pastRuns.map(_.filter(_.run.runId == r).map(_.run))

  val route =
    get {
      pathPrefix("v2") {
        path("runconfigurations" / Segment) { runId =>
          complete {
            byRun(RunId(runId))
          }
        } ~
          path("runconfigurations") {
            complete {
              state.pastRuns.map(_.map(_.run))
            }
          } ~
          path("analyses") {
            complete {
              state.analyses.map(_.assignments.toSeq)
            }
          } ~
          path("analyses" / Segment) { projectName =>
            complete {
              state.analyses.map(_.assignments.toSeq
                .filter {
                  case (project, _) =>
                    project == projectName
                })
            }
          } ~
          path("analyses" / Segment / Segment) { (projectName, analysisId) =>
            complete {
              state.analyses.map(_.assignments.toSeq
                .filter {
                  case (project, analyses) =>
                    project == projectName && analyses.exists(
                      _.analysisId == analysisId)
                })
            }
          }
      }
    }

}
