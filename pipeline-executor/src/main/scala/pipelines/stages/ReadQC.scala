package org.gc.pipelines.stages

import tasks._
import tasks.collection._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import scala.concurrent.Future
import org.gc.readqc
import org.gc.pipelines.model._
import org.gc.pipelines.util.{
  ResourceConfig,
  ReadQCPlot,
  Exec,
  JVM,
  StableSet,
  traverseAll
}
import org.gc.pipelines.util.StableSet.syntax
import akka.stream.scaladsl.Source
import akka.util.ByteString

case class ReadQCPerUnitInput(fastqs: StableSet[FastQ])
    extends WithSharedFiles(fastqs.toSeq.flatMap(_.files): _*)

case class ReadQCMetrics(metrics: readqc.Metrics)

case class ReadQCResult(metrics: EValue[PerSamplePerLanePerReadMetrics],
                        plots: SharedFile,
                        gcFractionTable: SharedFile)
    extends WithSharedFiles(plots)

case class ReadQCInput(samples: StableSet[PerSampleFastQ], title: String)
    extends WithSharedFiles(samples.toSeq.flatMap(_.files): _*)

object ReadQC {

  def extractReadQCJar(): String =
    fileutils.TempFile
      .getExecutableFromJar("/readqc", "readqc")
      .getAbsolutePath

  def makeGCTable(
      metrics: Seq[
        (Project, SampleId, RunId, Lane, ReadType, readqc.Metrics)]) = {
    val body = metrics
      .map {
        case (project, sampleId, runId, lane, readType, metrics) =>
          Seq(project,
              sampleId,
              runId,
              lane,
              readType,
              metrics.gcFraction,
              metrics.readNumber).mkString(",")
      }
      .mkString("\n")
    val header = "project,sample,run,lane,read_type,gc_fraction,read_number"
    header + "\n" + body + "\n"
  }

  val readQC = AsyncTask[ReadQCInput, ReadQCResult]("__readqc", 1) {
    case ReadQCInput(samples, title) =>
      implicit computationEnvironment =>
        releaseResources

        val units = for {
          sample <- samples.toSeq
          fastqPartitiosOfLane <- sample.lanes.toSeq
            .groupBy(_.lane)
            .values
            .toSeq
          read <- List((fastqPartitiosOfLane.map(_.read1), ReadType(1)),
                       (fastqPartitiosOfLane.map(_.read2), ReadType(2)))
        } yield
          (
            sample.project,
            sample.sampleId,
            fastqPartitiosOfLane.head.runId,
            fastqPartitiosOfLane.head.lane,
            read._2,
            read._1
          )

        def plotMetricsFile(
            metrics: Seq[
              (Project, SampleId, RunId, Lane, ReadType, readqc.Metrics)])
          : Future[SharedFile] = {
          val plot = ReadQCPlot.make(metrics, title)
          SharedFile(plot, s"$title.readqc.pdf", true)
        }

        for {

          metrics <- traverseAll(units) {
            case (project, sample, run, lane, read, fastqs) =>
              for {
                metrics <- readQCPerUnit(
                  ReadQCPerUnitInput(fastqs.toSet.toStable))(
                  ResourceConfig.readQC)
              } yield (project, sample, run, lane, read, metrics.metrics)
          }

          metricsPlots <- plotMetricsFile(metrics)
          metricsFile <- EValue(PerSamplePerLanePerReadMetrics(metrics),
                                title + ".readqc.js")
          gcTable <- SharedFile(Source.single(ByteString(makeGCTable(metrics))),
                                title + ".gcfraction.txt")
        } yield ReadQCResult(metricsFile, metricsPlots, gcTable)

  }

  val readQCPerUnit =
    AsyncTask[ReadQCPerUnitInput, ReadQCMetrics]("__readqc-unit", 1) {
      case ReadQCPerUnitInput(fastqs) =>
        implicit computationEnvironment =>
          log.info("read qc of fastq files: " + fastqs)

          val readQCJar = extractReadQCJar()

          for {
            fastqFiles <- Future.traverse(fastqs.toSeq)(_.file.file)
            metrics = {
              val (stdout, _, _) =
                Exec.bash(logDiscriminator = "readqc",
                          onError = Exec.ThrowIfNonZero)(s""" \\
                        cat ${fastqFiles
                  .map(_.getAbsolutePath)
                  .mkString(" ")} | \\
                        gunzip -c | \\
                        java ${JVM.serial} -Xmx5G -jar $readQCJar 
                        """)
              io.circe.parser.decode[readqc.Metrics](stdout.mkString).right.get
            }
          } yield ReadQCMetrics(metrics)

    }
}

object ReadQCMetrics {
  implicit val encoder: Encoder[ReadQCMetrics] =
    deriveEncoder[ReadQCMetrics]
  implicit val decoder: Decoder[ReadQCMetrics] =
    deriveDecoder[ReadQCMetrics]
}

object ReadQCInput {
  implicit val encoder: Encoder[ReadQCInput] =
    deriveEncoder[ReadQCInput]
  implicit val decoder: Decoder[ReadQCInput] =
    deriveDecoder[ReadQCInput]
}

object ReadQCPerUnitInput {
  implicit val encoder: Encoder[ReadQCPerUnitInput] =
    deriveEncoder[ReadQCPerUnitInput]
  implicit val decoder: Decoder[ReadQCPerUnitInput] =
    deriveDecoder[ReadQCPerUnitInput]
}

object ReadQCResult {
  import io.circe.generic.auto._
  implicit val encoder: Encoder[ReadQCResult] =
    deriveEncoder[ReadQCResult]
  implicit val decoder: Decoder[ReadQCResult] =
    deriveDecoder[ReadQCResult]
}
