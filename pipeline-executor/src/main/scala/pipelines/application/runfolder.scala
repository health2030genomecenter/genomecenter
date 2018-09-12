package org.gc.pipelines.application

import akka.stream.scaladsl.{Source, Merge}
import akka.stream.alpakka.file.scaladsl.DirectoryChangesSource
import akka.stream.alpakka.file.DirectoryChange
import scala.concurrent.duration._
import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import java.nio.file.FileSystems
import java.io.File
import fileutils.openSource
import com.typesafe.scalalogging.StrictLogging

import org.gc.pipelines.model.SampleSheet

case class RunfolderReadyForProcessing(runId: String,
                                       sampleSheet: SampleSheet,
                                       runFolderPath: String)

case class ProcessingFinished(run: RunfolderReadyForProcessing,
                              success: Boolean)

trait SequencingCompleteEventSource {
  def events: Source[RunfolderReadyForProcessing, _]
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

/** Listens on file creation events from the filesystem
  *
  * Writing data to a file and creating the file are not atomic operations,
  * thus this method is prone to race condition unless the file is linked (moved) atomically to the
  * destination path.
  */
case class FolderWatcherEventSource(folderWhereRunFoldersArePlaced: String,
                                    fileSignalingCompletion: String,
                                    sampleSheetFileName: String)
    extends SequencingCompleteEventSource
    with StrictLogging {
  private val fs = FileSystems.getDefault

  private def readFolder(runFolder: File): RunfolderReadyForProcessing = {
    val sampleSheet = SampleSheet(
      openSource(new File(runFolder, sampleSheetFileName))(_.mkString))
    val runId = runFolder.getAbsoluteFile.getName
    RunfolderReadyForProcessing(runId, sampleSheet, runFolder.getAbsolutePath)
  }

  def events: Source[RunfolderReadyForProcessing, _] =
    DirectoryChangesSource(fs.getPath(folderWhereRunFoldersArePlaced),
                           pollInterval = 1 second,
                           maxBufferSize = 1000)
      .flatMapConcat {
        case (potentialRunFolder, DirectoryChange.Creation) =>
          DirectoryChangesSource(potentialRunFolder,
                                 pollInterval = 1 second,
                                 maxBufferSize = 1000)
        case _ => Source.empty
      }
      .map { event =>
        logger.debug(s"Directory change event $event")
        event
      }
      .collect {
        case (fileInRunFolder, DirectoryChange.Creation)
            if fileInRunFolder.getFileName.toString == fileSignalingCompletion =>
          val runFolder = fileInRunFolder.toFile.getParentFile
          readFolder(runFolder)
      }

}

object RunfolderReadyForProcessing {
  implicit val encoder: Encoder[RunfolderReadyForProcessing] =
    deriveEncoder[RunfolderReadyForProcessing]
  implicit val decoder: Decoder[RunfolderReadyForProcessing] =
    deriveDecoder[RunfolderReadyForProcessing]
}
