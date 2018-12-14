package org.gc.pipelines.application

import scala.concurrent.Future
import com.typesafe.scalalogging.StrictLogging
import java.io.File
import io.circe.{Encoder, Decoder, Json, Printer}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.gc.pipelines.model.RunId

class Storage[T: Encoder: Decoder](file: File, migrations: Seq[Json => Json])
    extends StrictLogging {

  val currentVersion = migrations.size

  case class Entry[K](
      schemaVersion: Int,
      data: K
  )
  object Entry {
    implicit def encoder[K: Encoder]: Encoder[Entry[K]] =
      deriveEncoder[Entry[K]]
    implicit def decoder[K: Decoder]: Decoder[Entry[K]] =
      deriveDecoder[Entry[K]]
  }

  private val printer = Printer.noSpaces.copy(dropNullValues = true)

  def append(e: T) = synchronized {
    import io.circe.syntax._

    val data = printer.pretty(Entry(currentVersion, e).asJson)
    // The expected frequency of appends are low, thus instead of keeping a
    // file descriptor open for a very long time
    // we open it for write each time.
    val writer = new java.io.FileWriter(file, true) // append = true
    writer.write(data)
    writer.write("\n")
    writer.flush
    writer.close
  }

  private def readToJson =
    fileutils.openSource(file)(_.getLines.toList.map { line =>
      io.circe.parser.parse(line).right.get
    })

  private def migrateFromVersion(version: Int, json: Json): T =
    if (version == currentVersion)
      implicitly[Decoder[T]].decodeJson(json).right.get
    else {
      val migrated = migrations(version)(json)
      logger.debug(s"Migrated ${json.noSpaces} to ${migrated.noSpaces}")
      migrateFromVersion(version + 1, migrated)
    }

  def read = {
    readToJson.map { json =>
      val version = json.hcursor.downField("schemaVersion").as[Int].right.get
      migrateFromVersion(version, json.hcursor.downField("data").focus.get)
    }

  }
}

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

  val storage =
    new Storage[Event](logFile, PipelineStateMigrations.migrations)

  private def recover() =
    storage.read
      .map { e =>
        logger.debug(s"Recovered migrated event as $e")
        e
      }
      .foreach(updateState)

  val writer = new java.io.FileWriter(logFile, true) // append = true

  private def appendEvent(e: Event) =
    storage.append(e)

  recover()

  def updateState(e: Event) = e match {
    case Registered(r) =>
      past = r :: past
    case Deleted(runId) =>
      past =
        past.filterNot(runFolder => (runFolder.runId: RunId) == (runId: RunId))
  }

  def registered(r: RunfolderReadyForProcessing) = synchronized {
    logger.info(s"Registering run ${r.runId}")
    val event = Registered(r)
    appendEvent(event)
    updateState(event)
    Future.successful(Some(r))
  }

  def invalidated(runId: RunId) = synchronized {
    val event = Deleted(runId)
    appendEvent(event)
    Future.successful(())
  }

  def pastRuns =
    Future.successful(past)

  def contains(runId: RunId) =
    Future.successful(past.exists(_.runId == runId))

}
