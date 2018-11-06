package org.gc.pipelines.stages

import tasks._
import tasks.collection._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import scala.concurrent.Future
import org.gc.readqc
import org.gc.pipelines.model._
import org.gc.pipelines.util.{ResourceConfig, ReadQCPlot}

import akka.stream.scaladsl.StreamConverters
import scala.collection.JavaConverters._

case class ReadQCPerUnitInput(fastqs: Set[FastQ])
    extends WithSharedFiles(fastqs.toSeq.flatMap(_.files): _*)

case class ReadQCMetrics(metrics: readqc.Metrics)

case class ReadQCResult(metrics: EValue[PerSamplePerLanePerReadMetrics],
                        plots: SharedFile)
    extends WithSharedFiles(plots)

case class ReadQCInput(samples: Set[PerSampleFastQ], runId: RunId)
    extends WithSharedFiles(samples.toSeq.flatMap(_.files): _*)

object ReadQC {

  val readQC = AsyncTask[ReadQCInput, ReadQCResult]("__readqc", 1) {
    case ReadQCInput(samples, runId) =>
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
            sample.sampleId,
            fastqPartitiosOfLane.head.lane,
            read._2,
            read._1
          )

        def plotMetricsFile(
            metrics: Seq[(SampleId, Lane, ReadType, readqc.Metrics)])
          : Future[SharedFile] = {
          val plot = ReadQCPlot.make(metrics)
          SharedFile(plot, s"$runId.reads.pdf", true)
        }

        for {

          metrics <- Future.traverse(units) {
            case (sample, lane, read, fastqs) =>
              for {
                metrics <- readQCPerUnit(ReadQCPerUnitInput(fastqs.toSet))(
                  ResourceConfig.readQC)
              } yield (sample, lane, read, metrics.metrics)
          }

          metricsPlots <- plotMetricsFile(metrics)
          metricsFile <- EValue(PerSamplePerLanePerReadMetrics(metrics),
                                "readQC." + runId + ".js")
        } yield ReadQCResult(metricsFile, metricsPlots)

  }

  val readQCPerUnit =
    AsyncTask[ReadQCPerUnitInput, ReadQCMetrics]("__readqc-unit", 1) {
      case ReadQCPerUnitInput(fastqs) =>
        implicit computationEnvironment =>
          implicit val materializer =
            computationEnvironment.components.actorMaterializer

          log.info("read qc of fastq files: " + fastqs)

          val fastqSource = fastqs.toSeq.map(_.file.source).reduce(_ ++ _)
          val reader = new java.io.BufferedReader(
            new java.io.InputStreamReader(
              new java.util.zip.GZIPInputStream(
                fastqSource.runWith(StreamConverters.asInputStream()),
                65536)))
          val fqIterator =
            new htsjdk.samtools.fastq.FastqReader(reader).iterator
          val metrics = readqc.ReadQC.processHtsJdkRecords(fqIterator.asScala)

          Future.successful(ReadQCMetrics(metrics))

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
