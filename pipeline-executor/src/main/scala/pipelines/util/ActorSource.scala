package org.gc.pipelines.util

import akka.actor._
import akka.stream.scaladsl._
import akka.stream._
import com.typesafe.scalalogging.StrictLogging

object ActorSource extends StrictLogging {
  private class Forwarder extends Actor {
    var listeners: List[ActorRef] = Nil
    override def postStop: Unit = {
      logger.error("Stopping ActorSource forwarder")
      listeners.foreach(_ ! PoisonPill)
    }
    def receive = {
      case actorRef: ActorRef =>
        listeners = actorRef :: listeners
      case other =>
        listeners.foreach(_ ! other)
    }
  }

  /* A convenient way to make a Source which can be fed from an actor
   *
   * The price of the convenience is that the actor is started before the
   * stream is materialized (as opposed to normal Source.actorRef)
   */
  def make[T](implicit AS: ActorSystem) = {
    val fw = AS.actorOf(Props[Forwarder])
    val source = Source
      .actorRef[T](bufferSize = 1000000,
                   overflowStrategy = OverflowStrategy.fail)
      .mapMaterializedValue { actorRef =>
        fw ! actorRef

      }
      .watchTermination() {
        case (mat, future) =>
          future.onComplete {
            case result =>
              result.failed.foreach { e =>
                logger.error("Unexpected exception ", e)
              }
              logger.info("ActorSource terminated.")
          }(AS.dispatcher)
          mat
      }
    val close = () => fw ! PoisonPill
    (fw, source, close)
  }
}
