package org.gc.pipelines.application

import akka.stream.scaladsl.{Source, Merge}

trait SequencingCompleteEventSource {
  def events: Source[RunfolderReadyForProcessing, _]
}

object EmptySequencingCompleteEventSource
    extends SequencingCompleteEventSource {
  def events = Source.empty[RunfolderReadyForProcessing]
}

case class CompositeSequencingCompleteEventSource(
    first: SequencingCompleteEventSource,
    second: SequencingCompleteEventSource,
    rest: SequencingCompleteEventSource*)
    extends SequencingCompleteEventSource {
  def events =
    Source.combine(first.events, second.events, rest.map(_.events): _*)(count =>
      Merge(count))
}
