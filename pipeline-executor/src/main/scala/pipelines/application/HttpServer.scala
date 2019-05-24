package org.gc.pipelines.application

import com.typesafe.scalalogging.StrictLogging
import akka.stream.Materializer
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.{Directives, Route}
import scala.concurrent.ExecutionContext

trait HttpComponent {
  def route: Route
}

class HttpServer(port: Int, components: Seq[Route])(implicit AS: ActorSystem,
                                                    MAT: Materializer,
                                                    EC: ExecutionContext)
    extends StrictLogging {

  val route =
    Directives.concat(components: _*)

  lazy val startServer
    : scala.concurrent.Future[akka.http.scaladsl.Http.ServerBinding] =
    Http()
      .bindAndHandle(route, "0.0.0.0", port)
      .andThen {
        case scala.util.Success(binding) =>
          logger.info(s"Server bound to $binding")
        case _ =>
      }

}
