package org.gc.pipelines.application

import com.typesafe.scalalogging.StrictLogging
import org.gc.pipelines.util.ActorSource
import org.gc.pipelines.model.RunId
import akka.stream.Materializer
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import java.io.File

import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

class HttpServer(implicit AS: ActorSystem, MAT: Materializer)
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

  private[pipelines] val route =
    post {
      path("delete" / Segment) { runId =>
        sourceActor ! Delete(RunId(runId))
        complete(akka.http.scaladsl.model.StatusCodes.OK)
      } ~
        path("runfolder") {
          entity(as[Seq[RunfolderDTO]]) { runFolders =>
            val invalidPath = runFolders.exists {
              case RunfolderDTO(path, _) =>
                !new java.io.File(path).canRead ||
                  !new java.io.File(path, "RunInfo.xml").canRead
            }

            if (invalidPath) {
              complete(akka.http.scaladsl.model.StatusCodes.BadRequest)
            } else {
              runFolders.foreach {
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

              }
              complete(akka.http.scaladsl.model.StatusCodes.OK)
            }
          }

        }
    }

  val port = 9099

  private val bindingFuture =
    Http().bindAndHandle(route, "0.0.0.0", port)

  bindingFuture.andThen {
    case scala.util.Success(serverBinding) =>
      logger.info(
        s"Pipeline application's web server is listening on ${serverBinding.localAddress}")
  }(AS.dispatcher)

}
