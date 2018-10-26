package org.gc.pipelines.application

import akka.stream.scaladsl.Source
import akka.stream.alpakka.file.scaladsl.DirectoryChangesSource
import akka.stream.alpakka.file.DirectoryChange
import scala.concurrent.duration._
import java.nio.file.FileSystems
import java.io.File
import com.typesafe.scalalogging.StrictLogging

/** Listens on file creation events from the filesystem
  *
  * Writing data to a file and creating the file are not atomic operations,
  * thus this method is prone to race condition unless the file is linked (moved) atomically to the
  * destination path.
  */
case class FolderWatcherEventSource(folderWhereRunFoldersArePlaced: String,
                                    fileSignalingCompletion: String,
                                    configFileFolder: File)
    extends SequencingCompleteEventSource
    with StrictLogging {
  private val fs = FileSystems.getDefault

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
          val parsedRunFolder =
            RunfolderReadyForProcessing.readFolder(runFolder, configFileFolder)
          parsedRunFolder.left.foreach { error =>
            logger.info(s"$runFolder failed to parse due to error $error.")
          }
          parsedRunFolder
      }
      .collect {
        case Right(runFolderReady) => runFolderReady
      }

}
