package org.gc.pipelines.application

import scala.concurrent.Future
import com.typesafe.scalalogging.StrictLogging
import java.io.File
import io.circe.{Encoder, Decoder, Json, Printer}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.gc.pipelines.model.{RunId, AnalysisId, Project}

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
      val parsed = io.circe.parser.parse(line)

      parsed.left.foreach { e =>
        logger.error(s"Failed to parse json from $file \n$line", e)
      }
      parsed.right.get
    })

  private def migrateFromVersion(version: Int, json: Json): T =
    if (version == currentVersion)
      implicitly[Decoder[T]].decodeJson(json).toTry.get
    else {
      val migrated = migrations(version)(json)
      logger.debug(s"Migrated ${json.noSpaces} to ${migrated.noSpaces}")
      migrateFromVersion(currentVersion, migrated)
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
  private var past = Vector[RunfolderReadyForProcessing]()
  private var runOrder = Vector[RunId]()
  private var _analyses = AnalysisAssignments.empty
  sealed trait Event
  case class Registered(run: RunfolderReadyForProcessing) extends Event
  case class Deleted(runId: RunId) extends Event
  case class Assigned(project: Project, analysis: AnalysisConfiguration)
      extends Event
  case class Unassigned(project: Project, analysis: AnalysisId) extends Event

  object Event {
    implicit val encoder: Encoder[Event] =
      deriveEncoder[Event]
    implicit val decoder: Decoder[Event] =
      deriveDecoder[Event]
  }

  val storage =
    new Storage[Event](logFile, PipelineStateMigrations.migrations)

  private def recover() = {
    storage.read
      .map { e =>
        logger.debug(s"Recovered migrated event as $e")
        e
      }
      .foreach(updateState)

    past.foreach { runFolder =>
      logger.info(s"State after recovery: $runFolder")
    }
  }

  val writer = new java.io.FileWriter(logFile, true) // append = true

  private def appendEvent(e: Event) =
    storage.append(e)

  recover()

  def updateState(e: Event) = e match {
    case Registered(r) =>
      if (!runOrder.contains(r.runId)) {
        past = past :+ r
        runOrder = runOrder :+ r.runId
      } else {
        past = (past.filterNot(_.runId == r.runId) :+ r).sortBy(r =>
          runOrder.indexOf(r.runId))
      }
    case Deleted(runId) =>
      past =
        past.filterNot(runFolder => (runFolder.runId: RunId) == (runId: RunId))
    case Assigned(project, analysis) =>
      _analyses = _analyses.assigned(project, analysis)
    case Unassigned(project, analysis) =>
      _analyses = _analyses.unassigned(project, analysis)
  }

  def registered(r: RunfolderReadyForProcessing) = synchronized {
    logger.info(s"Registering run ${r.runId}")
    val event = Registered(r)
    appendEvent(event)
    updateState(event)
    Future.successful(Some(RunWithAnalyses(r, _analyses)))
  }

  def invalidated(runId: RunId) = synchronized {
    val event = Deleted(runId)
    appendEvent(event)
    Future.successful(())
  }

  def pastRuns =
    Future.successful(past.toList.map(run => RunWithAnalyses(run, _analyses)))

  def analyses = Future.successful(_analyses)

  def contains(runId: RunId) =
    Future.successful(past.exists(_.runId == runId))

  def assigned(project: Project,
               analysisConfiguration: AnalysisConfiguration): Future[Unit] = {
    logger.info(s"Assigning $project to $analysisConfiguration")
    val event = Assigned(project, analysisConfiguration)
    appendEvent(event)
    updateState(event)
    Future.successful(())
  }
  def unassigned(project: Project, analysisId: AnalysisId): Future[Unit] = {
    logger.info(s"Unassigning $project to $analysisId")
    val event = Unassigned(project, analysisId)
    appendEvent(event)
    updateState(event)
    Future.successful(())
  }

}
