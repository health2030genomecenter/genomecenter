package org.gc.pipelines.application

import com.typesafe.scalalogging.StrictLogging
import org.gc.pipelines.util.ActorSource
import org.gc.pipelines.model.RunId
import akka.stream.Materializer
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RejectionHandler
import java.io.File

import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import com.typesafe.config.ConfigFactory
import scala.util.Try

class HttpServer(port: Int)(implicit AS: ActorSystem, MAT: Materializer)
    extends StrictLogging
    with SequencingCompleteEventSource {

  val (sourceActor, source, closeSource) =
    ActorSource.make[Command]

  val commands = source

  case class RunfolderDTO(path: String, configurationFilePath: String)
  object RunfolderDTO {
    implicit val encoder: Encoder[RunfolderDTO] =
      deriveEncoder[RunfolderDTO]
    implicit val decoder: Decoder[RunfolderDTO] =
      deriveDecoder[RunfolderDTO]
  }

  val rejectionHandler =
    RejectionHandler
      .newBuilder()
      .handle {
        case reject =>
          logger.info("Rejected with: " + reject.toString)
          complete(
            HttpResponse(StatusCodes.BadRequest,
                         entity = s"Rejection: $reject"))
      }
      .result()

  private[pipelines] val route =
    handleRejections(rejectionHandler) {
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
                complete(HttpResponse(StatusCodes.BadRequest,
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
                        logger.info(
                          s"$dto failed to parse due to error $error.")
                      case Right(runFolderReadyEvent) =>
                        sourceActor ! Append(runFolderReadyEvent)
                    }

                    maybeRunFolderReadyEvent
                }

                if (parsed.forall(_.isRight))
                  complete(akka.http.scaladsl.model.StatusCodes.OK)
                else
                  complete(
                    HttpResponse(StatusCodes.BadRequest,
                                 entity = HttpEntity(
                                   parsed.map(_.toString).mkString("\n"))))
              }
            }

          } ~
          path("v2" / "register") {
            entity(as[String]) { runFolderConfigurationAsString =>
              val maybeRunFolderReadyEvent = for {
                parsed <- Try(ConfigFactory.parseString(
                  runFolderConfigurationAsString)).toEither.left
                  .map(_.toString)
                runFolder <- RunfolderReadyForProcessing.fromConfig(parsed)
              } yield runFolder

              maybeRunFolderReadyEvent match {
                case Left(error) =>
                  logger.error(error.toString)
                  complete(
                    HttpResponse(StatusCodes.BadRequest,
                                 entity = HttpEntity(error)))
                case Right(runFolder) if runFolder.validationErrors.nonEmpty =>
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

  lazy val startServer
    : scala.concurrent.Future[akka.http.scaladsl.Http.ServerBinding] =
    Http()
      .bindAndHandle(route, "0.0.0.0", port)

}
