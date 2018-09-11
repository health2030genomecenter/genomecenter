package org.gc.pipelines.util

import akka.actor._
import akka.stream.scaladsl._
import akka.stream._

object ActorSource {
  private class Forwarder extends Actor {
    var listeners: List[ActorRef] = Nil
    override def postStop: Unit = {
      listeners.foreach(_ ! PoisonPill)
    }
    def receive = {
      case actorRef: ActorRef =>
        listeners = actorRef :: listeners
      case other =>
        listeners.foreach(_ ! other)
    }
  }
  def make[T](implicit AS: ActorSystem) = {
    val fw = AS.actorOf(Props[Forwarder])
    val source = Source
      .actorRef[T](bufferSize = 1000000,
                   overflowStrategy = OverflowStrategy.fail)
      .mapMaterializedValue { actorRef =>
        fw ! actorRef

      }
    val close = () => fw ! PoisonPill
    (fw, source, close)
  }
}
