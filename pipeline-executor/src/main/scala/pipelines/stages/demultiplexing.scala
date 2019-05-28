package org.gc.pipelines.stages

import org.gc.pipelines.model._
import org.gc.pipelines.util.{Exec, Files, ResourceConfig, traverseAll}
import org.gc.pipelines.util.StableSet
import org.gc.pipelines.util.StableSet.syntax

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
    runFolderPath: String,
    sampleSheet: SampleSheetFile,
    extraBcl2FastqCliArguments: Seq[String],
    globalIndexSet: Option[SharedFile],
    partitionByLane: Option[Boolean],
    noPartition: Option[Boolean],
    partitionByTileCount: Option[Int],
    createIndexFastqInReadType: Option[ReadType]
) extends WithSharedFiles(sampleSheet.files ++ globalIndexSet.toSeq: _*) {
  def readLengths: Map[ReadType, Int] =
    DemultiplexingInput.parseReadLengthFromBcl2FastqArguments(
      extraBcl2FastqCliArguments)
}

case class DemultiplexSingleLaneInput(run: DemultiplexingInput,
                                      tiles: StableSet[String],
                                      partitionIndex: Int)
    extends WithSharedFiles(run.files: _*)

case class DemultiplexedReadData(fastqs: StableSet[FastQWithSampleMetadata],
                                 stats: EValue[DemultiplexingStats.Root])
    extends ResultWithSharedFiles(
      fastqs.toSeq.map(_.fastq.file) ++ stats.files: _*) {
  def withoutUndetermined =
    DemultiplexedReadData(
      fastqs.filterNot(_.sampleId == SampleId("Undetermined")),
      stats)
}

object Demultiplexing {

  def sampleToProjectMap(metadata: Seq[DemultiplexedReadData]) =
    (for {
      lanes <- metadata
      sample <- lanes.fastqs.toSeq
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

  def parseTiles(file: File): Seq[String] = {
    val content = fileutils.openSource(file)(_.mkString)
    parseTiles(content)
  }

  def parseTiles(s: String) = {
    val root = scala.xml.XML.loadString(s)
    (root \ "Run" \ "FlowcellLayout" \ "TileSet" \ "Tiles" \ "Tile").map {
      node =>
        node.text
    }.toSeq

  }

  val allLanes =
    AsyncTask[DemultiplexingInput, DemultiplexedReadData]("__demultiplex", 3) {
      runFolder => implicit computationEnvironment =>
        releaseResources
        implicit val actorMaterializer =
          computationEnvironment.components.actorMaterializer

        val partitionByLane = runFolder.partitionByLane match {
          case None       => true
          case Some(bool) => bool
        }

        val noPartition = runFolder.noPartition.exists(identity)

        def tilesOfLanes(lanes: Seq[Lane]): Seq[String] = {
          val runInfo = new File(runFolder.runFolderPath + "/RunInfo.xml")
          val allTiles = Demultiplexing.parseTiles(runInfo)
          allTiles.filter(tile =>
            lanes.exists(lane => tile.startsWith(lane.toString)))
        }

        for {
          sampleSheet <- runFolder.sampleSheet.parse
          globalIndexSet <- runFolder.globalIndexSet
            .map(sf => parseGlobalIndexSet(sf.source))
            .getOrElse(Future.successful(readGlobalIndexSetFromClassPath))

          partitions = {
            val lanes = {
              val lanes = sampleSheet.lanes
              if (lanes.isEmpty) {
                log.error("No lanes in the sample sheet!")
              }
              lanes
            }
            val tiles = tilesOfLanes(lanes)

            log.info(s"Found tiles of lanes $lanes: " + tiles.mkString(", "))
            if (noPartition) List(lanes.map(_.toString))
            else {
              if (tiles.isEmpty || partitionByLane)
                // process full lane by lane
                lanes.map(l => List(l.toString)).toSeq
              else
                // process by groups of tiles
                tiles
                  .grouped(runFolder.partitionByTileCount.getOrElse(60))
                  .toSeq
            }
          }

          demultiplexedLanes <- traverseAll(partitions.toSeq.zipWithIndex) {
            case (tiles, idx) =>
              perLane(
                DemultiplexSingleLaneInput(runFolder,
                                           tiles.toSet.toStable,
                                           idx))(ResourceConfig.bcl2fastq)

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

          mergedStatsInFile <- for {
            _ <- {
              val demultiplexingSummary = DemultiplexingSummary.fromStats(
                mergedStats,
                sampleToProjectMap(demultiplexedLanes.toSeq),
                globalIndexSet)
              val tableAsString =
                DemultiplexingSummary.renderAsTable(demultiplexingSummary)
              SharedFile(
                Source.single(ByteString(tableAsString.getBytes("UTF-8"))),
                name = demultiplexingSummary.runId + ".demultiplexing.stats.txt")
            }
            mergedStatsEValue <- EValue.apply(mergedStats, "Stats.json")
          } yield mergedStatsEValue
        } yield
          DemultiplexedReadData(
            demultiplexedLanes.flatMap(_.fastqs.toSeq).toSet.toStable,
            mergedStatsInFile)

    }

  val fastqFileNameRegex =
    "^([a-zA-Z0-9_\\-\\/]+/)?([a-zA-Z0-9_-]+)_S([0-9]+)_L([0-9]*)_([RI])([0-9])_001.fastq.gz$".r

  val perLane =
    AsyncTask[DemultiplexSingleLaneInput, DemultiplexedReadData](
      "__demultiplex-per-lane",
      5) {
      case DemultiplexSingleLaneInput(
          dmInput @ DemultiplexingInput(runFolderPath,
                                        sampleSheet,
                                        extraArguments,
                                        _,
                                        _,
                                        _,
                                        _,
                                        createIndexFastqInReadType),
          tilesToProcess,
          partitionIndex
          ) =>
        implicit computationEnvironment =>
          implicit val materializer =
            computationEnvironment.components.actorMaterializer

          val executable =
            fileutils.TempFile.getExecutableFromJar(resourceName =
                                                      "/bin/bcl2fastq_v220",
                                                    fileName = "bcl2fastq_v220")

          val createIndexFastqInUmi = createIndexFastqInReadType.isDefined

          def extractMetadataFromFilename(fastq: SharedFile,
                                          sampleSheet: SampleSheet.ParsedData,
                                          stats: DemultiplexingStats.Root) =
            fastq.name match {
              case fastqFileNameRegex(_,
                                      _sampleName,
                                      sampleNumberInSampleSheet1Based,
                                      lane,
                                      readOrIndex,
                                      read) =>
                if (sampleNumberInSampleSheet1Based.toInt > 0) {
                  val sampleSheetEntries =
                    sampleSheet.getSampleSheetEntriesByBcl2FastqSampleNumber(
                      sampleNumberInSampleSheet1Based.toInt)
                  require(sampleSheetEntries.map(_.sampleId).distinct.size == 1)
                  require(
                    sampleSheetEntries.map(_.sampleName).distinct.size == 1)
                  require(sampleSheetEntries.map(_.project).distinct.size == 1)

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

                  val readType =
                    if (readOrIndex == "R") ReadType(read.toInt)
                    else createIndexFastqInReadType.getOrElse(ReadType(0))

                  val readLength =
                    if (readOrIndex == "R") Some(dmInput.readLengths(readType))
                    else None

                  Some(
                    FastQWithSampleMetadata(
                      sampleSheetProject,
                      sampleSheetSampleId,
                      parsedLane,
                      readType,
                      PartitionId(partitionIndex),
                      FastQ(fastq, numberOfReads.get, readLength)
                    ))
                } else {
                  val numberOfReads = for {
                    laneStats <- stats.ConversionResults.find(
                      _.LaneNumber == lane.toInt)
                  } yield laneStats.Undetermined.NumberReads

                  Some(
                    FastQWithSampleMetadata(
                      Project("NA"),
                      SampleId("Undetermined"),
                      Lane(lane.toInt),
                      ReadType(read.toInt),
                      PartitionId(partitionIndex),
                      FastQ(fastq, numberOfReads.get, None)))
                }
              case _ =>
                log.error(
                  s"Fastq file name has unexpected pattern. $fastq name: ${fastq.name}")
                None
            }

          val tilesArgumentList = if (extraArguments.contains("--tiles")) {
            log.warning("--tiles argument to bcl2fastq is overriden.")
            Nil
          } else
            List("--tiles",
                 tilesToProcess.toSeq.sorted.map(t => "s_" + t).mkString(","))

          /* bcl2fastq manual page 16:
           * Use one thread per CPU core plus a little more to supply CPU with work.
           */
          val processingThreads = resourceAllocated.cpu + 2

          def inPartFolder[T] =
            appendToFilePrefix[T](Seq("part" + partitionIndex.toString))

          for {
            sampleSheetFile <- sampleSheet.file.file
            parsedSampleSheet <- sampleSheet.parse

            fastQAndStatFiles <- inPartFolder {
              implicit computationEnvironment =>
                val outputFolder =
                  Files.createTempFile(s".$partitionIndex.bcl2fastq")
                outputFolder.delete
                outputFolder.mkdirs
                val stdout = new File(outputFolder, "stdout").getAbsolutePath
                val stderr = new File(outputFolder, "stderr").getAbsolutePath

                val createIndexArgument =
                  if (createIndexFastqInUmi)
                    List("--create-fastq-for-index-reads")
                  else Nil

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
                  ) ++ tilesArgumentList ++ extraArguments ++ createIndexArgument

                  val escaped = commandLine.mkString("'", "' '", "'")
                  s""" $escaped 1> $stdout 2> $stderr """
                }

                val (_, _, exitCode) =
                  Exec.bash(logDiscriminator = "bcl2fastq")(bashCommand)
                if (exitCode != 0) {
                  val stdErrContents = fileutils.openSource(stderr)(_.mkString)
                  log.error(
                    "bcl2fastq failed. stderr follows:\n" + stdErrContents)
                  throw new RuntimeException(
                    s"bcl2fastq exited with code != 0 stderr: \n $stdErrContents")
                }

                val statsFile =
                  new File(new File(outputFolder, "Stats"), "Stats.json")

                val files = Files.list(outputFolder, "**.fastq.gz") :+ statsFile :+ new File(
                  stdout) :+ new File(stderr)

                files.foreach(_.deleteOnExit)

                Future.traverse(files) { file =>
                  SharedFile(file,
                             name = file.getAbsolutePath
                               .drop(outputFolder.getAbsolutePath.size)
                               .stripPrefix("/"),
                             deleteFile = true)
                }

            }

            statsFileContent <- fastQAndStatFiles
              .dropRight(2) // drop stderr and stdout files
              .last
              .source
              .runFold(ByteString.empty)(_ ++ _)
              .map(_.utf8String)

          } yield {
            val fastQFiles = fastQAndStatFiles.dropRight(3)
            val statsFile = fastQAndStatFiles.dropRight(2).last
            val stats =
              io.circe.parser
                .decode[DemultiplexingStats.Root](statsFileContent)
                .right
                .get

            DemultiplexedReadData(
              fastQFiles
                .map(extractMetadataFromFilename(_, parsedSampleSheet, stats))
                .collect { case Some(tuple) => tuple }
                .toSet
                .toStable,
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

  def parseReadLengthFromBcl2FastqArguments(
      arguments: Seq[String]): Map[ReadType, Int] = {

    def parsingLoop(input: List[Char],
                    subLength: Int,
                    subChar: Option[Char],
                    acc: List[(Char, Int)]): Either[String, List[(Char, Int)]] =
      input match {
        case Nil if subChar.isDefined =>
          Right((subChar.get, subLength) :: acc)
        case Nil => Right(acc)
        case c :: cs if c.isDigit && subChar.isDefined =>
          val (lenString, remaining) = (c :: cs).span(_.isDigit)
          parsingLoop(remaining,
                      lenString.mkString.toInt + subLength - 1,
                      subChar,
                      acc)
        case c :: _ if c.isDigit =>
          Left("Illegal input. Expected non digit. " + input.mkString)
        case c :: cs if subChar.isDefined && subChar.get == c =>
          parsingLoop(cs, subLength + 1, Some(c), acc)
        case c :: cs if subChar.isDefined =>
          parsingLoop(cs, 1, Some(c), (subChar.get, subLength) :: acc)
        case c :: cs => parsingLoop(cs, 1, Some(c), acc)

      }

    val idx = arguments.indexOf("--use-bases-mask")
    if (arguments.size >= idx + 2) {
      val argument = arguments(idx + 1)
      val spl = argument.split(",")
      val sections = spl.map { section =>
        val parsedSection = parsingLoop(section.toSeq.toList, 0, None, Nil)
        parsedSection match {
          case Left(error) => throw new RuntimeException(error)
          case Right(parsedSection) =>
            parsedSection
              .filter(_._1 == 'y')
              .map(_._2)
              .sum

        }

      }
      sections
        .filter(_ > 0)
        .zipWithIndex
        .map {
          case (length, idx) =>
            ReadType(idx + 1) -> length
        }
        .toMap

    } else Map.empty
  }
}
