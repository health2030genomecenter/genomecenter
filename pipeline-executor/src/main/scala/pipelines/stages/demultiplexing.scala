package org.gc.pipelines.stages

import org.gc.pipelines.model._
import org.gc.pipelines.util.{Exec, Files, ResourceConfig}

import scala.concurrent.Future
import tasks._
import tasks.collection._
import tasks.circesupport._
import io.circe._
import io.circe.generic.semiauto._

import akka.stream.scaladsl.Source
import akka.stream.Materializer
import akka.util.ByteString
import scala.concurrent.ExecutionContext

import java.io.File

case class DemultiplexingInput(
    processingId: ProcessingId,
    runId: RunId,
    runFolderPath: String,
    sampleSheet: SampleSheetFile,
    extraBcl2FastqCliArguments: Seq[String],
    globalIndexSet: Option[SharedFile]
) extends WithSharedFiles(sampleSheet.files ++ globalIndexSet.toSeq: _*)

case class DemultiplexSingleLaneInput(run: DemultiplexingInput, lane: Lane)

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

  def parseGlobalIndexSet(source: Source[ByteString, _])(
      implicit am: Materializer,
      ec: ExecutionContext) =
    source.runFold(ByteString.empty)(_ ++ _).map(_.utf8String).map { string =>
      scala.io.Source.fromString(string).getLines.filter(_.nonEmpty).toSet
    }

  def readGlobalIndexSetFromClassPath = {
    val s = scala.io.Source
      .fromInputStream(getClass.getResourceAsStream("/truseq_index.txt"))
    val r = s.getLines.filter(_.nonEmpty).toSet
    s.close
    r
  }

  val allLanes =
    AsyncTask[DemultiplexingInput, DemultiplexedReadData]("__demultiplex", 2) {
      runFolder => implicit computationEnvironment =>
        releaseResources
        computationEnvironment.withFilePrefix(Seq("demultiplex")) {
          implicit computationEnvironment =>
            implicit val actorMaterializer =
              computationEnvironment.components.actorMaterializer

            def intoRunIdFolder[T] =
              appendToFilePrefix[T](
                Seq(runFolder.runId, runFolder.processingId))

            for {
              sampleSheet <- runFolder.sampleSheet.parse
              globalIndexSet <- runFolder.globalIndexSet
                .map(sf => parseGlobalIndexSet(sf.source))
                .getOrElse(Future.successful(readGlobalIndexSetFromClassPath))

              lanes = {
                val lanes = sampleSheet.lanes
                if (lanes.isEmpty) {
                  log.error("No lanes in the sample sheet!")
                }
                lanes
              }

              demultiplexedLanes <- Future.traverse(lanes) { lane =>
                perLane(DemultiplexSingleLaneInput(runFolder, lane))(
                  ResourceConfig.bcl2fastq)
              }

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
                            sampleToProjectMap(demultiplexedLanes),
                            globalIndexSet))
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
    "^([a-zA-Z0-9_\\-\\/]+/)?([a-zA-Z0-9_-]+)_S([0-9]+)_L([0-9]*)_(R[12])_001.fastq.gz$".r

  val perLane =
    AsyncTask[DemultiplexSingleLaneInput, DemultiplexedReadData](
      "__demultiplex-per-lane",
      5) {
      case DemultiplexSingleLaneInput(
          DemultiplexingInput(processingId,
                              runId,
                              runFolderPath,
                              sampleSheet,
                              extraArguments,
                              _),
          laneToProcess
          ) =>
        implicit computationEnvironment =>
          implicit val materializer =
            computationEnvironment.components.actorMaterializer

          val executable =
            fileutils.TempFile.getExecutableFromJar(resourceName =
                                                      "/bin/bcl2fastq_v220",
                                                    fileName = "bcl2fastq_v220")

          def extractMetadataFromFilename(fastq: SharedFile,
                                          sampleSheet: SampleSheet.ParsedData,
                                          stats: DemultiplexingStats.Root) =
            fastq.name match {
              case fastqFileNameRegex(_,
                                      _sampleName,
                                      sampleNumberInSampleSheet1Based,
                                      lane,
                                      read) =>
                if (sampleNumberInSampleSheet1Based.toInt > 0) {
                  val sampleSheetEntries =
                    sampleSheet.getSampleSheetEntriesByBcl2FastqSampleNumber(
                      sampleNumberInSampleSheet1Based.toInt)
                  assert(sampleSheetEntries.map(_.sampleId).distinct.size == 1)
                  assert(
                    sampleSheetEntries.map(_.sampleName).distinct.size == 1)
                  assert(sampleSheetEntries.map(_.project).distinct.size == 1)

                  val sampleSheetSampleId = sampleSheetEntries.head.sampleId
                  val sampleSheetSampleName = sampleSheetEntries.head.sampleName
                  val sampleSheetProject = sampleSheetEntries.head.project
                  val sampleSheetLanes: Seq[Lane] =
                    sampleSheetEntries.map(_.lane)

                  val parsedSampleName = SampleName(_sampleName)
                  val parsedLane = Lane(lane.toInt)

                  require(
                    sampleSheetSampleName == parsedSampleName,
                    s"Sample name parsed from file name and read from sample sheet do not match. $sampleSheetSampleName $parsedSampleName $fastq $sampleSheet"
                  )
                  require(
                    sampleSheetLanes.contains(Lane(lane.toInt)),
                    s"lanes parsed from file name and read from sample sheet do not match. $sampleSheetLanes $lane $fastq $sampleSheet"
                  )

                  val numberOfReads = for {
                    laneStats <- stats.ConversionResults.find(
                      _.LaneNumber == parsedLane)
                    sampleStats <- laneStats.DemuxResults.find(
                      _.SampleId == sampleSheetSampleId)
                  } yield sampleStats.NumberReads

                  Some(
                    FastQWithSampleMetadata(sampleSheetProject,
                                            sampleSheetSampleId,
                                            RunId(runId),
                                            parsedLane,
                                            ReadType(read),
                                            FastQ(fastq, numberOfReads.get)))
                } else {
                  val numberOfReads = for {
                    laneStats <- stats.ConversionResults.find(
                      _.LaneNumber == lane.toInt)
                  } yield laneStats.Undetermined.NumberReads

                  Some(
                    FastQWithSampleMetadata(Project("NA"),
                                            SampleId("Undetermined"),
                                            RunId(runId),
                                            Lane(lane.toInt),
                                            ReadType(read),
                                            FastQ(fastq, numberOfReads.get)))
                }
              case _ =>
                log.error(
                  s"Fastq file name has unexpected pattern. $fastq name: ${fastq.name}")
                None
            }

          val tilesArgumentList = if (extraArguments.contains("--tiles")) {
            log.warning("--tiles argument to bcl2fastq is overriden.")
            Nil
          } else List("--tiles", "s_" + laneToProcess)

          val processingThreads = resourceAllocated.cpu

          for {
            sampleSheetFile <- sampleSheet.file.file
            parsedSampleSheet <- sampleSheet.parse

            fastQAndStatFiles <- SharedFile.fromFolder { outputFolder =>
              val stdout = new File(outputFolder, "stdout").getAbsolutePath
              val stderr = new File(outputFolder, "stderr").getAbsolutePath
              val bashCommand = {
                val commandLine = Seq(
                  executable.getAbsolutePath,
                  "--runfolder-dir",
                  runFolderPath,
                  "--output-dir",
                  outputFolder.getAbsolutePath,
                  "--sample-sheet",
                  sampleSheetFile.getAbsolutePath,
                  "--processing-threads",
                  processingThreads.toString
                ) ++ tilesArgumentList ++ extraArguments

                val escaped = commandLine.mkString("'", "' '", "'")
                s""" $escaped 1> $stdout 2> $stderr """
              }

              val (_, _, exitCode) =
                Exec.bash(logDiscriminator = "bcl2fastq." + runId)(bashCommand)
              if (exitCode != 0) {
                val stdErrContents = fileutils.openSource(stderr)(_.mkString)
                log.error(
                  "bcl2fastq failed. stderr follows:\n" + stdErrContents)
                throw new RuntimeException("bcl2fastq exited with code != 0")
              }

              val statsFile =
                new File(new File(outputFolder, "Stats"), "Stats.json")

              Files.list(outputFolder, "**.fastq.gz") :+ statsFile

            }(
              computationEnvironment.components
                .withChildPrefix(runId)
                .withChildPrefix(processingId)
                .withChildPrefix(laneToProcess.toString))

            statsFileContent <- fastQAndStatFiles.last.source
              .runFold(ByteString.empty)(_ ++ _)
              .map(_.utf8String)

          } yield {
            val fastQFiles = fastQAndStatFiles.dropRight(1)
            val statsFile = fastQAndStatFiles.last
            val stats =
              io.circe.parser
                .decode[DemultiplexingStats.Root](statsFileContent)
                .right
                .get

            DemultiplexedReadData(
              fastQFiles
                .map(extractMetadataFromFilename(_, parsedSampleSheet, stats))
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

object DemultiplexingInput {
  implicit val encoder: Encoder[DemultiplexingInput] =
    deriveEncoder[DemultiplexingInput]
  implicit val decoder: Decoder[DemultiplexingInput] =
    deriveDecoder[DemultiplexingInput]
}
