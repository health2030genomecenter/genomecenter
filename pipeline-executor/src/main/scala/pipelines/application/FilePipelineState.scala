package org.gc.pipelines.application

import scala.concurrent.Future
import com.typesafe.scalalogging.StrictLogging
import java.io.File
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.gc.pipelines.model.RunId

class FilePipelineState(logFile: File)
    extends PipelineState
    with StrictLogging {
  private var past = List[RunfolderReadyForProcessing]()
  sealed trait Event
  case class Registered(run: RunfolderReadyForProcessing) extends Event
  case class Deleted(runId: RunId) extends Event

  object Event {
    implicit val encoder: Encoder[Event] =
      deriveEncoder[Event]
    implicit val decoder: Decoder[Event] =
      deriveDecoder[Event]
  }

  private def recover() = {
    fileutils.openSource(logFile)(_.getLines.foreach { line =>
      updateState(io.circe.parser.decode[Event](line).right.get)
    })

    logger.info(s"Recovery completed. Past runs: $past")
  }

  val writer = new java.io.FileWriter(logFile, true) // append = true

  private def appendEvent(e: Event) = synchronized {
    import io.circe.syntax._
    val data = e.asJson.noSpaces
    writer.write(data)
    writer.write("\n")
    writer.flush
  }

  recover()

  def updateState(e: Event) = e match {
    case Registered(r) =>
      past = r :: past
    case Deleted(runId) =>
      past = past.filterNot(_.runId == runId)
  }

  def registered(r: RunfolderReadyForProcessing) = synchronized {
    logger.info(s"Registering run ${r.runId}")
    val event = Registered(r)
    appendEvent(event)
    updateState(event)
    Future.successful(Some(r))
  }

  def deleted(runId: RunId) = synchronized {
    val event = Deleted(runId)
    appendEvent(event)
    updateState(event)
    Future.successful(())
  }

  def pastRuns =
    Future.successful(past)

  def contains(runId: RunId) =
    Future.successful(past.exists(_.runId == runId))

}
