package org.gc.pipelines.stages

import org.gc.pipelines.application.RunfolderReadyForProcessing
import org.gc.pipelines.model._
import org.gc.pipelines.util.{Exec, Files}

import scala.concurrent.Future
import tasks._
import tasks.circesupport._
import io.circe._
import io.circe.generic.semiauto._

import java.io.File
case class DemultiplexSingleLaneInput(run: RunfolderReadyForProcessing,
                                      lane: Lane)

case class DemultiplexedReadData(fastqs: Set[FastQWithSampleMetadata])
    extends ResultWithSharedFiles(fastqs.toList.map(_.fastq.file): _*)

object Demultiplexing {

  val allLanes =
    AsyncTask[RunfolderReadyForProcessing, DemultiplexedReadData]("demultiplex",
                                                                  1) {
      runFolder => implicit computationEnvironment =>
        releaseResources

        val lanes: Seq[Lane] = runFolder.sampleSheet.parsed.lanes

        for {
          demultiplexedLanes <- Future.sequence(lanes.map { lane =>
            perLane(DemultiplexSingleLaneInput(runFolder, lane))(
              CPUMemoryRequest(12, 40000))
          })
        } yield
          DemultiplexedReadData(demultiplexedLanes.flatMap(_.fastqs).toSet)

    }

  val fastqFileNameRegex =
    "([a-zA-Z0-9_-]+)_S[0-9]*_(R[0-9]*)_(R[12])_001.fastq.gz".r

  val perLane =
    AsyncTask[DemultiplexSingleLaneInput, DemultiplexedReadData](
      "demultiplex-per-lane",
      1) {
      case DemultiplexSingleLaneInput(
          RunfolderReadyForProcessing(runId, sampleSheet, runFolderPath),
          laneToProcess
          ) =>
        implicit computationEnvironment =>
          val extraArguments = sampleSheet.parsed.extraBcl2FastqCliArguments

          val executable = fileutils.TempFile.getExecutableFromJar("/bcl2fastq")

          val laneNumber = laneToProcess.dropWhile(_ == 'L').toInt

          val fastQFilesF = SharedFile.fromFolder { outputFolder =>
            val bashCommand = {
              val stdout = new File(outputFolder, "stdout").getAbsolutePath
              val stderr = new File(outputFolder, "stderr").getAbsolutePath
              val commandLine = Seq(
                executable.getAbsolutePath,
                "--runfolder-dir",
                runFolderPath,
                "--output-dir",
                outputFolder.getAbsolutePath,
                "--tiles",
                "s_" + laneNumber
              ) ++ extraArguments

              val escaped = commandLine.mkString("'", " ", "'")
              s""" $escaped 1> $stdout 2> $stderr """
            }

            Exec.bash(logDiscriminator = "bcl2fastq")(bashCommand)

            Files.list(outputFolder, "*.fastq.gz")

          }(computationEnvironment.components
            .withChildPrefix(runId)
            .withChildPrefix(laneToProcess))

          def extractMetadataFromFilename(fastq: SharedFile) =
            fastq.name match {
              case fastqFileNameRegex(_sampleId, lane, read) =>
                val sampleId = SampleId(_sampleId)
                val projectId = sampleSheet.parsed
                  .getProjectBySampleId(sampleId)
                  .getOrElse(Project("NA"))
                Some(
                  FastQWithSampleMetadata(projectId,
                                          sampleId,
                                          RunId(runId),
                                          Lane(lane),
                                          ReadType(read),
                                          FastQ(fastq)))
              case _ =>
                log.error(s"Fastq file name has unexpected pattern. $fastq")
                None
            }

          for {
            fastQFiles <- fastQFilesF
          } yield {
            DemultiplexedReadData(
              fastQFiles
                .map(extractMetadataFromFilename)
                .collect { case Some(tuple) => tuple }
                .toSet
            )
          }

    }

}

object DemultiplexSingleLaneInput {
  implicit val encoder: Encoder[DemultiplexSingleLaneInput] =
    deriveEncoder[DemultiplexSingleLaneInput]
  implicit val decoder: Decoder[DemultiplexSingleLaneInput] =
    deriveDecoder[DemultiplexSingleLaneInput]
}

object DemultiplexedReadData {
  implicit val encoder: Encoder[DemultiplexedReadData] =
    deriveEncoder[DemultiplexedReadData]
  implicit val decoder: Decoder[DemultiplexedReadData] =
    deriveDecoder[DemultiplexedReadData]
}
