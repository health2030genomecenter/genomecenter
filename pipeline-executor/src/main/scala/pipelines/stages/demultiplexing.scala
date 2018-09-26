package org.gc.pipelines.stages

import org.gc.pipelines.application.RunfolderReadyForProcessing
import org.gc.pipelines.model._
import org.gc.pipelines.util.{Exec, Files, ResourceConfig}

import scala.concurrent.Future
import tasks._
import tasks.circesupport._
import io.circe._
import io.circe.generic.semiauto._

import java.io.File
case class DemultiplexSingleLaneInput(run: RunfolderReadyForProcessing,
                                      lane: Lane)

case class DemultiplexedReadData(fastqs: Set[FastQWithSampleMetadata])
    extends ResultWithSharedFiles(fastqs.toList.map(_.fastq.file): _*) {
  def withoutUndetermined =
    DemultiplexedReadData(
      fastqs.filterNot(_.sampleId == SampleId("Undetermined")))
}

object Demultiplexing {

  val allLanes =
    AsyncTask[RunfolderReadyForProcessing, DemultiplexedReadData](
      "__demultiplex",
      1) { runFolder => implicit computationEnvironment =>
      releaseResources
      computationEnvironment.withFilePrefix(Seq("demultiplex")) {
        implicit computationEnvironment =>
          val lanes: Seq[Lane] = runFolder.sampleSheet.parsed.lanes

          if (lanes.isEmpty) {
            log.error("No lanes in the sample sheet!")
          }

          for {
            demultiplexedLanes <- Future.sequence(lanes.map { lane =>
              perLane(DemultiplexSingleLaneInput(runFolder, lane))(
                ResourceConfig.bcl2fastq)
            })
          } yield
            DemultiplexedReadData(demultiplexedLanes.flatMap(_.fastqs).toSet)
      }
    }

  val fastqFileNameRegex =
    "([a-zA-Z0-9_-]+)_S[0-9]*_(L[0-9]*)_(R[12])_001.fastq.gz".r

  val perLane =
    AsyncTask[DemultiplexSingleLaneInput, DemultiplexedReadData](
      "__demultiplex-per-lane",
      1) {
      case DemultiplexSingleLaneInput(
          RunfolderReadyForProcessing(runId, sampleSheet, runFolderPath),
          laneToProcess
          ) =>
        implicit computationEnvironment =>
          val extraArguments = sampleSheet.parsed.extraBcl2FastqCliArguments

          val executable =
            fileutils.TempFile.getExecutableFromJar(resourceName =
                                                      "/bin/bcl2fastq_v220",
                                                    fileName = "bcl2fastq_v220")

          val laneNumber = laneToProcess.dropWhile(_ == 'L').toInt

          val tilesArgumentList = if (extraArguments.contains("--tiles")) {
            log.warning("--tiles argument to bcl2fastq is overriden.")
            Nil
          } else List("--tiles", "s_" + laneNumber)

          val fastQFilesF = SharedFile.fromFolder { outputFolder =>
            val stdout = new File(outputFolder, "stdout").getAbsolutePath
            val stderr = new File(outputFolder, "stderr").getAbsolutePath
            val bashCommand = {
              val commandLine = Seq(
                executable.getAbsolutePath,
                "--runfolder-dir",
                runFolderPath,
                "--output-dir",
                outputFolder.getAbsolutePath
              ) ++ tilesArgumentList ++ extraArguments

              val escaped = commandLine.mkString("'", "' '", "'")
              s""" $escaped 1> $stdout 2> $stderr """
            }

            val (_, _, exitCode) =
              Exec.bash(logDiscriminator = "bcl2fastq." + runId)(bashCommand)
            if (exitCode != 0) {
              val stdErrContents = fileutils.openSource(stderr)(_.mkString)
              log.error("bcl2fastq failed. stderr follows:\n" + stdErrContents)
              throw new RuntimeException("bcl2fastq exited with code != 0")
            }

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
                log.error(
                  s"Fastq file name has unexpected pattern. $fastq name: ${fastq.name}")
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
