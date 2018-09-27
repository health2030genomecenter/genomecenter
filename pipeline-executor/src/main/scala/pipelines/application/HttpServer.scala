package org.gc.pipelines.application

import com.typesafe.scalalogging.StrictLogging
import org.gc.pipelines.util.ActorSource
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
    ActorSource.make[RunfolderReadyForProcessing]

  val events = source

  case class RunfolderDTO(path: String, sampleSheetFolderPath: String)
  object RunfolderDTO {
    implicit val encoder: Encoder[RunfolderDTO] =
      deriveEncoder[RunfolderDTO]
    implicit val decoder: Decoder[RunfolderDTO] =
      deriveDecoder[RunfolderDTO]
  }

  private[pipelines] val route =
    post {
      path("runfolder") {
        entity(as[RunfolderDTO]) {
          case RunfolderDTO(runFolderPath, sampleSheetFolderPath) =>
            logger.info(s"Got $runFolderPath")
            val runFolder = new java.io.File(runFolderPath)
            if (runFolder.canRead) {
              sourceActor ! RunfolderReadyForProcessing.readFolder(
                runFolder,
                new File(sampleSheetFolderPath))
              complete(akka.http.scaladsl.model.StatusCodes.OK)
            } else complete(akka.http.scaladsl.model.StatusCodes.BadRequest)
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
