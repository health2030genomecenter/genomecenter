package org.gc.pipelines.application

import com.typesafe.scalalogging.StrictLogging
import org.gc.pipelines.util.ActorSource
import org.gc.pipelines.model.{RunId, Project, AnalysisId}
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import java.io.File
import scala.concurrent.Promise
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import com.typesafe.config.ConfigFactory
import scala.util.Try

class HttpCommandSource(implicit AS: ActorSystem)
    extends StrictLogging
    with CommandSource
    with HttpComponent {

  val (sourceActor, source, closeSource) =
    ActorSource.make[Command]

  val commands = source

  private val shutdownPromise = Promise[Unit]
  val shutdown = shutdownPromise.future

  case class RunfolderDTO(path: String, configurationFilePath: String)
  object RunfolderDTO {
    implicit val encoder: Encoder[RunfolderDTO] =
      deriveEncoder[RunfolderDTO]
    implicit val decoder: Decoder[RunfolderDTO] =
      deriveDecoder[RunfolderDTO]
  }

  val route =
    post {
      path("delete" / Segment) { runId =>
        sourceActor ! Delete(RunId(runId))
        complete(akka.http.scaladsl.model.StatusCodes.OK)
      } ~
        path("runfolder") {
          import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
          entity(as[Seq[RunfolderDTO]]) { runFolders =>
            val invalidPath = runFolders.exists {
              case RunfolderDTO(path, _) =>
                !new java.io.File(path).canRead ||
                  !new java.io.File(path, "RunInfo.xml").canRead
            }

            if (invalidPath) {
              complete(
                HttpResponse(StatusCodes.BadRequest,
                             entity = HttpEntity("can't read")))
            } else {
              val parsed = runFolders.map {
                case dto @ RunfolderDTO(runFolderPath, configurationFile) =>
                  logger.info(s"Got $runFolderPath")
                  val runFolder = new java.io.File(runFolderPath)

                  val maybeRunFolderReadyEvent =
                    RunfolderReadyForProcessing.readFolderWithConfigFile(
                      runFolder,
                      new File(configurationFile)
                    )

                  maybeRunFolderReadyEvent match {
                    case Left(error) =>
                      logger.info(s"$dto failed to parse due to error $error.")
                    case Right(runFolderReadyEvent) =>
                      sourceActor ! Append(runFolderReadyEvent)
                  }

                  maybeRunFolderReadyEvent
              }

              if (parsed.forall(_.isRight))
                complete(akka.http.scaladsl.model.StatusCodes.OK)
              else
                complete(
                  HttpResponse(
                    StatusCodes.BadRequest,
                    entity = HttpEntity(parsed.map(_.toString).mkString("\n"))))
            }
          }

        }
    } ~
      pathPrefix("v2") {
        post {
          path("shutdown") {
            closeSource()
            shutdownPromise.trySuccess(())
            complete(akka.http.scaladsl.model.StatusCodes.OK)
          } ~
            path("reprocess") {
              sourceActor ! ReprocessAllRuns
              complete(akka.http.scaladsl.model.StatusCodes.OK)
            } ~
            path("analyses" / Segment) { project =>
              entity(as[String]) { analysisConfigurationAsString =>
                val parsedFromJson = io.circe.parser
                  .decode[AnalysisConfiguration](analysisConfigurationAsString)

                val maybeParsed =
                  if (parsedFromJson.isRight)
                    parsedFromJson.left.map(_.toString)
                  else
                    AnalysisConfiguration.fromConfig(
                      ConfigFactory.parseString(analysisConfigurationAsString))

                maybeParsed match {
                  case Left(error) =>
                    logger.error(error.toString)
                    complete(HttpResponse(StatusCodes.BadRequest,
                                          entity = HttpEntity(error)))
                  case Right(configuration)
                      if configuration.validationErrors.nonEmpty =>
                    logger.error(s"Can't read $configuration")
                    complete(HttpResponse(
                      StatusCodes.BadRequest,
                      entity = HttpEntity(
                        s"can't read: ${configuration.validationErrors.mkString(";")}")))
                  case Right(configuration) =>
                    logger.info(
                      s"Assign ${configuration.analysisId} - $project")
                    sourceActor ! Assign(Project(project), configuration)
                    complete(akka.http.scaladsl.model.StatusCodes.OK)
                }

              }
            }
        } ~
          delete {
            path("analyses" / Segment / Segment) {
              case (project, analysisId) =>
                logger.info(s"Unassign $analysisId - $project")
                sourceActor ! Unassign(Project(project), AnalysisId(analysisId))
                complete(akka.http.scaladsl.model.StatusCodes.OK)

            }
          } ~
          delete {
            path("runs" / Segment) { runId =>
              sourceActor ! Delete(RunId(runId))
              complete(akka.http.scaladsl.model.StatusCodes.OK)
            }
          } ~
          post {
            path("runs") {
              entity(as[String]) { runFolderConfigurationAsString =>
                val parsedFromJson =
                  io.circe.parser.decode[RunfolderReadyForProcessing](
                    runFolderConfigurationAsString)

                val maybeRunFolderReadyEvent =
                  if (parsedFromJson.isRight)
                    parsedFromJson.left.map(_.toString)
                  else
                    for {
                      parsed <- Try(ConfigFactory.parseString(
                        runFolderConfigurationAsString)).toEither.left
                        .map(_.toString)
                      runFolder <- RunfolderReadyForProcessing.fromConfig(
                        parsed)
                    } yield runFolder

                maybeRunFolderReadyEvent match {
                  case Left(error) =>
                    logger.error(error.toString)
                    complete(HttpResponse(StatusCodes.BadRequest,
                                          entity = HttpEntity(error)))
                  case Right(runFolder)
                      if runFolder.validationErrors.nonEmpty =>
                    logger.error(s"Can't read $runFolder")
                    complete(HttpResponse(
                      StatusCodes.BadRequest,
                      entity = HttpEntity(
                        s"can't read: ${runFolder.validationErrors.mkString(";")}")))
                  case Right(runFolder) =>
                    logger.info(s"Got ${runFolder.runId}")
                    sourceActor ! Append(runFolder)
                    complete(akka.http.scaladsl.model.StatusCodes.OK)
                }

              }

            }
          }
      }

}
