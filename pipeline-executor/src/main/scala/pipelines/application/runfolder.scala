package org.gc.pipelines.application

import akka.stream.scaladsl.{Source, Merge}
import akka.stream.alpakka.file.scaladsl.DirectoryChangesSource
import akka.stream.alpakka.file.DirectoryChange
import scala.concurrent.duration._
import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import java.nio.file.FileSystems
import java.io.File
import com.typesafe.scalalogging.StrictLogging
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._

case class RunConfiguration(
    automatic: Boolean,
    sampleSheet: String,
    referenceFasta: String,
    targetIntervals: String,
    bqsrKnownSites: Set[String],
    extraBcl2FastqArguments: Seq[String],
    /* Mapping between members of a read pair and numbers assigned by bcl2fastq */
    readAssignment: (Int, Int),
    /* Number assigned by bcl2fastq, if any */
    umi: Option[Int]
)

case class RunfolderReadyForProcessing(runId: String,
                                       runFolderPath: String,
                                       runConfiguration: RunConfiguration)

case class ProcessingFinished(run: RunfolderReadyForProcessing,
                              success: Boolean)

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

object RunfolderReadyForProcessing {
  implicit val encoder: Encoder[RunfolderReadyForProcessing] =
    deriveEncoder[RunfolderReadyForProcessing]
  implicit val decoder: Decoder[RunfolderReadyForProcessing] =
    deriveDecoder[RunfolderReadyForProcessing]

  def readFolder(
      runFolder: File,
      configFileFolder: File): Either[String, RunfolderReadyForProcessing] = {

    val runId = runFolder.getAbsoluteFile.getName
    val runConfigurationFile = new File(configFileFolder, "config-" + runId)
    RunConfiguration(runConfigurationFile).map(
      runConfiguration =>
        RunfolderReadyForProcessing(runId,
                                    runFolder.getAbsolutePath,
                                    runConfiguration))
  }

  def readFolderWithConfigFile(runFolder: File, runConfigurationFile: File)
    : Either[String, RunfolderReadyForProcessing] = {

    val runId = runFolder.getAbsoluteFile.getName
    RunConfiguration(runConfigurationFile).map(
      runConfiguration =>
        RunfolderReadyForProcessing(runId,
                                    runFolder.getAbsolutePath,
                                    runConfiguration))
  }
}

object RunConfiguration {
  implicit val encoder: Encoder[RunConfiguration] =
    deriveEncoder[RunConfiguration]
  implicit val decoder: Decoder[RunConfiguration] =
    deriveDecoder[RunConfiguration]

  def apply(content: String): Either[String, RunConfiguration] =
    scala.util
      .Try {
        val config = ConfigFactory.parseString(content)
        RunConfiguration(
          automatic = config.getBoolean("automatic"),
          referenceFasta = config.getString("referenceFasta"),
          targetIntervals = config.getString("targetIntervals"),
          bqsrKnownSites = config.getStringList("bqsr.knownSites").asScala.toSet,
          extraBcl2FastqArguments =
            config.getStringList("extraBcl2FastqArguments").asScala,
          sampleSheet = config.getString("sampleSheet"),
          readAssignment = {
            val list = config.getIntList("readAssignment").asScala
            (list(0), list(1))
          },
          umi =
            config.getIntList("umiReadNumber").asScala.headOption.map(_.toInt)
        )
      }
      .toEither
      .left
      .map(_.toString)

  def apply(file: File): Either[String, RunConfiguration] =
    apply(fileutils.openSource(file)(_.mkString))

}
