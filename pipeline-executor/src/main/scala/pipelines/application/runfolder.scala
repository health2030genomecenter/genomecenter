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

case class FolderWatcherEventSource(path: String,
                                    fileName: String,
                                    sampleSheetFileName: String)
    extends SequencingCompleteEventSource {
  private val fs = FileSystems.getDefault

  private def readFolder(runFolder: File): RunfolderReadyForProcessing = {
    val sampleSheet = SampleSheet(
      openSource(new File(runFolder, sampleSheetFileName))(_.mkString))
    val runId = runFolder.getAbsoluteFile.getName
    RunfolderReadyForProcessing(runId, sampleSheet, runFolder.getAbsolutePath)
  }

  def events: Source[RunfolderReadyForProcessing, _] =
    DirectoryChangesSource(fs.getPath(path),
                           pollInterval = 1 second,
                           maxBufferSize = 1000).collect {
      case (path, DirectoryChange.Creation)
          if path.getFileName.toString == fileName =>
        readFolder(path.toFile)
    }

}

object RunfolderReadyForProcessing {
  implicit val encoder: Encoder[RunfolderReadyForProcessing] =
    deriveEncoder[RunfolderReadyForProcessing]
  implicit val decoder: Decoder[RunfolderReadyForProcessing] =
    deriveDecoder[RunfolderReadyForProcessing]
}
