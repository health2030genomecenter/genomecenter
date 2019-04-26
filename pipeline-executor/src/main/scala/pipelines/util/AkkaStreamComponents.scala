package org.gc.pipelines.util

import akka.stream.scaladsl._
import akka.stream.FlowShape
import akka.http.scaladsl.server.RejectionHandler
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.model._
import com.typesafe.scalalogging.StrictLogging

object AkkaStreamComponents extends StrictLogging {

  def broadcastThenMerge[I, O1, O2](
      flow1: Flow[I, O1, _],
      flow2: Flow[I, O2, _]): Flow[I, Either[O1, O2], _] =
    Flow.fromGraph(GraphDSL.create(flow1, flow2)((_, _)) {
      implicit builder => (flow1, flow2) =>
        import GraphDSL.Implicits._

        val broadcast = builder.add(Broadcast[I](2))
        val merge = builder.add(Merge[Either[O1, O2]](2))

        broadcast.out(0) ~> flow1 ~> Flow[O1].map(Left(_)) ~> merge.in(0)
        broadcast.out(1) ~> flow2 ~> Flow[O2].map(Right(_)) ~> merge.in(1)

        FlowShape(broadcast.in, merge.out)
    })

  def deduplicateBy[T, K](fun: T => K): Flow[T, T, _] =
    Flow[T]
      .mapConcat(t => List(t, t))
      .zipWithIndex
      .sliding(2, 1)
      .mapConcat { list =>
        val (previous, idx) = list(0)
        val current = list(1)._1
        if (idx == 0)
          List(previous)
        else if (fun(previous) == fun(current)) Nil
        else List(current)

      }
  def deduplicate[T]: Flow[T, T, _] =
    deduplicateBy(identity)

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

}
