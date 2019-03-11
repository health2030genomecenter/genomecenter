package org.gc.pipelines.application

import com.typesafe.scalalogging.StrictLogging
import akka.stream.Materializer
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.{Directives, Route}

trait HttpComponent {
  def route: Route
}

class HttpServer(port: Int, components: Seq[Route])(implicit AS: ActorSystem,
                                                    MAT: Materializer)
    extends StrictLogging {

  val route =
    Directives.concat(components: _*)

  lazy val startServer
    : scala.concurrent.Future[akka.http.scaladsl.Http.ServerBinding] =
    Http()
      .bindAndHandle(route, "0.0.0.0", port)

}
