package org.gc.pipelines.application

import akka.http.scaladsl.server.Directives._
import scala.concurrent.ExecutionContext
import org.gc.pipelines.model._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import tasks.TaskSystemComponents
import java.io.File
import akka.stream.scaladsl.{Source, StreamConverters}
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging

class ConfigurationQueryHttpComponent(state: PipelineState)(
    implicit ec: ExecutionContext,
    tsc: TaskSystemComponents)
    extends HttpComponent
    with StrictLogging {

  def byRun(r: RunId) =
    state.pastRuns.map(_.filter(_.run.runId == r).map(_.run))

  val fileSystemRoot = tsc.tasksConfig.storageURI.getPath

  def findFastqFiles(runId: RunId): Source[String, _] = {

    def walk(directory: File): Source[java.nio.file.Path, _] =
      if (java.nio.file.Files.isDirectory(directory.toPath)) {
        val factory = () => java.nio.file.Files.walk(directory.toPath)
        StreamConverters.fromJavaStream(factory)
      } else Source.empty

    val root = new java.io.File(fileSystemRoot + "/demultiplexing/" + runId)
    walk(root)
      .filter(_.getFileName.endsWith("fastq.gz"))
      .map(_.toFile.getAbsolutePath)
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
                val responseDataF = for {
                  assignments <- state.analyses
                  runs <- state.pastRuns
                } yield {
                  val projectsWithAssignments =
                    assignments.assignments.filter(_._2.nonEmpty).keySet

                  val runIds =
                    runs
                      .filter {
                        case RunWithAnalyses(run, _) =>
                          (projectsWithAssignments & run.projects.toSet).isEmpty
                      }
                      .map {
                        case RunWithAnalyses(run, _) =>
                          run.runId
                      }

                  val sourceOfResponseData: Source[ByteString, _] = {
                    import io.circe.syntax._
                    import io.circe.generic.auto._
                    val sourceOfEithers =
                      if (withFastQ.isDefined)
                        Source(runIds).flatMapConcat(runId =>
                          findFastqFiles(runId).map(path =>
                            Right((runId, path))))
                      else Source.apply(runIds).map(Left(_))
                    sourceOfEithers.map(_.asJson.noSpaces).map(ByteString(_))
                  }

                  sourceOfResponseData.watchTermination() {
                    case (mat, future) =>
                      future.onComplete {
                        case result =>
                          result.failed.foreach { e =>
                            logger.error("Unexpected exception ", e)
                          }
                      }
                      mat
                  }
                }

                akka.http.scaladsl.model.HttpEntity(
                  akka.http.scaladsl.model.ContentTypes.`application/octet-stream`,
                  Source.fromFutureSource(responseDataF))
              }
            }
          }
      }
    }

}
