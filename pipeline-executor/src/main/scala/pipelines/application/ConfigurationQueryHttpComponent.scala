package org.gc.pipelines.application

import akka.http.scaladsl.server.Directives._
import scala.concurrent.ExecutionContext
import org.gc.pipelines.model._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import tasks.TaskSystemComponents

class ConfigurationQueryHttpComponent(state: PipelineState)(
    implicit ec: ExecutionContext,
    tsc: TaskSystemComponents)
    extends HttpComponent {

  def byRun(r: RunId) =
    state.pastRuns.map(_.filter(_.run.runId == r).map(_.run))

  val fileSystemRoot = tsc.tasksConfig.storageURI.getPath

  def findFastqFiles(runId: RunId): List[String] = {
    import better.files._
    val root = new java.io.File(fileSystemRoot + "/demultiplexing/" + runId)
    root.toScala.glob("*.fastq.gz").map(_.toJava.getAbsolutePath).toList
  }

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
          path("analysed-projects") {
            complete {
              state.analyses
                .map { data =>
                  val projects: Seq[Project] =
                    data.assignments
                      .filter(_._2.nonEmpty)
                      .keySet
                      .toSeq
                      .sortBy(_.toString)
                  projects
                }

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
              state.analyses.map { assignments =>
                val ofProject =
                  assignments.assignments.get(Project(projectName))

                ofProject
                  .flatMap(_.find(_.analysisId == analysisId))
              }
            }
          } ~
          path("free-runs") {
            parameters("fastq".?) { withFastQ =>
              complete {
                for {
                  assignments <- state.analyses
                  runs <- state.pastRuns
                } yield {
                  val projectsWithAssignments =
                    assignments.assignments.filter(_._2.nonEmpty).keySet
                  runs
                    .filter {
                      case RunWithAnalyses(run, _) =>
                        (projectsWithAssignments & run.projects.toSet).isEmpty
                    }
                    .map {
                      case RunWithAnalyses(run, _) =>
                        val fqList =
                          if (withFastQ.nonEmpty) findFastqFiles(run.runId)
                          else Nil
                        (run.runId, fqList)
                    }
                }

              }
            }
          }
      }
    }

}
