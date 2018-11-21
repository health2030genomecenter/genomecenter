package org.gc.pipelines.application

import akka.stream.scaladsl.{Source, Merge}
import org.gc.pipelines.model.RunId

sealed trait Command

case class Append(runFolder: RunfolderReadyForProcessing) extends Command

case class Delete(runId: RunId) extends Command

trait SequencingCompleteEventSource {
  def commands: Source[Command, _]
}

object EmptySequencingCompleteEventSource
    extends SequencingCompleteEventSource {
  def commands = Source.empty[Command]
}

case class CompositeSequencingCompleteEventSource(
    first: SequencingCompleteEventSource,
    second: SequencingCompleteEventSource,
    rest: SequencingCompleteEventSource*)
    extends SequencingCompleteEventSource {
  def commands =
    Source.combine(first.commands, second.commands, rest.map(_.commands): _*)(
      count => Merge(count))
}
