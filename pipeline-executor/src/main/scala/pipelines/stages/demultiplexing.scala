package org.gc.pipelines.stages

import org.gc.pipelines.application.RunfolderReadyForProcessing
import org.gc.pipelines.model._
import org.gc.pipelines.util.{Exec, Files, ResourceConfig}

import scala.concurrent.Future
import tasks._
import tasks.collection._
import tasks.circesupport._
import io.circe._
import io.circe.generic.semiauto._

import akka.stream.scaladsl.Source
import akka.util.ByteString

import java.io.File
case class DemultiplexSingleLaneInput(run: RunfolderReadyForProcessing,
                                      lane: Lane)

case class DemultiplexedReadData(fastqs: Set[FastQWithSampleMetadata],
                                 stats: EValue[DemultiplexingStats.Root])
    extends ResultWithSharedFiles(
      fastqs.toList.map(_.fastq.file) ++ stats.files: _*) {
  def withoutUndetermined =
    DemultiplexedReadData(
      fastqs.filterNot(_.sampleId == SampleId("Undetermined")),
      stats)
}

object Demultiplexing {

  def sampleToProjectMap(metadata: Seq[DemultiplexedReadData]) =
    (for {
      lanes <- metadata
      sample <- lanes.fastqs
    } yield (sample.sampleId -> sample.project)).toMap

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

          def intoRunIdFolder[T] = appendToFilePrefix[T](Seq(runFolder.runId))

          for {
            demultiplexedLanes <- Future.sequence(lanes.map { lane =>
              perLane(DemultiplexSingleLaneInput(runFolder, lane))(
                ResourceConfig.bcl2fastq)
            })
            perLaneStats <- Future.traverse(demultiplexedLanes)(_.stats.get)
            mergedStats = perLaneStats
              .reduce { (x, y) =>
                val merged = (x ++ y)
                merged match {
                  case Left(error) =>
                    log.error(x.toString)
                    log.error(y.toString)
                    throw new RuntimeException(s"Can't merge because $error")
                  case Right(merged) => merged
                }
              }

            mergedStatsInFile <- intoRunIdFolder {
              implicit computationEnvironment =>
                for {
                  _ <- {
                    val tableAsString =
                      DemultiplexingSummary.renderAsTable(
                        DemultiplexingSummary.fromStats(
                          mergedStats,
                          sampleToProjectMap(demultiplexedLanes)))
                    SharedFile(Source.single(
                                 ByteString(tableAsString.getBytes("UTF-8"))),
                               name = runFolder.runId + ".stats.table")
                  }
                  mergedStatsEValue <- EValue.apply(
                    mergedStats,
                    runFolder.runId + ".Stats.json")
                } yield mergedStatsEValue
            }
          } yield
            DemultiplexedReadData(demultiplexedLanes.flatMap(_.fastqs).toSet,
                                  mergedStatsInFile)
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

          val fastQAndStatFilesF = SharedFile.fromFolder { outputFolder =>
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

            val statsFile =
              new File(new File(outputFolder, "Stats"), "Stats.json")

            Files.list(outputFolder, "*.fastq.gz") :+ statsFile

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
            fastQAndStatFiles <- fastQAndStatFilesF
          } yield {
            val fastQFiles = fastQAndStatFiles.dropRight(1)
            val statsFile = fastQAndStatFiles.last

            DemultiplexedReadData(
              fastQFiles
                .map(extractMetadataFromFilename)
                .collect { case Some(tuple) => tuple }
                .toSet,
              EValue(statsFile)
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
  import io.circe.generic.auto._
  implicit val encoder: Encoder[DemultiplexedReadData] =
    deriveEncoder[DemultiplexedReadData]
  implicit val decoder: Decoder[DemultiplexedReadData] =
    deriveDecoder[DemultiplexedReadData]
}
