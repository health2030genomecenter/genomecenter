package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import fileutils.TempFile
import org.gc.pipelines.util.Exec
import org.gc.pipelines.model._
import org.gc.pipelines.model.{FastpReport => FastpReportModel}
import java.io.File
import scala.concurrent.Future
import akka.stream.scaladsl.Source
import akka.util.ByteString

case class AlignmentQCInput(bam: CoordinateSortedBam,
                            reference: IndexedReferenceFasta)
    extends WithSharedFiles((bam.files ++ reference.files): _*)

case class SelectionQCInput(bam: CoordinateSortedBam,
                            reference: IndexedReferenceFasta,
                            selectionTargetIntervals: BedFile)
    extends WithSharedFiles(
      (bam.files ++ reference.files ++ selectionTargetIntervals.files): _*)

case class SelectionQCResult(hsMetrics: SharedFile)
    extends WithSharedFiles(hsMetrics)

case class AlignmentQCResult(
    alignmentSummary: SharedFile,
    biasDetail: SharedFile,
    biasSummary: SharedFile,
    errorSummary: SharedFile,
    preAdapterDetail: SharedFile,
    preAdapterSummary: SharedFile
) extends WithSharedFiles(alignmentSummary,
                            biasDetail,
                            biasSummary,
                            errorSummary,
                            preAdapterDetail,
                            preAdapterSummary)

case class SampleMetrics(alignmentSummary: SharedFile,
                         hsMetrics: SharedFile,
                         duplicationMetrics: SharedFile,
                         fastpReports: Seq[FastpReport],
                         project: Project,
                         sampleId: SampleId,
                         runId: RunId)
    extends WithSharedFiles(alignmentSummary, hsMetrics, duplicationMetrics)

case class RunQCTableInput(runId: RunId, samples: Seq[SampleMetrics])
    extends WithSharedFiles(samples.flatMap(_.files): _*)

case class RunQCTable(file: SharedFile) extends WithSharedFiles(file)

object AlignmentQC {

  def makeTable(
      metrics: Seq[(AlignmentSummaryMetrics.Root,
                    HsMetrics.Root,
                    DuplicationMetrics.Root,
                    FastpReportModel.Root)]): String = {
    val header =
      "Proj          Sample        Lane   CptrKit              TotRds   MeanTrgtCov %PfRds %PfRdsAligned %PfUqRdsAligned   %Dup   DupRds OptDupRds BadCycles %Chimera %TrgtBase10 %TrgtBase30 %TrgtBase50    GC InsertSizePeak"

    val lines = metrics
      .map {
        case (alignment, targetSelection, dups, fastpMetrics) =>
          import alignment.pairMetrics._
          import targetSelection.metrics._
          import dups.metrics._
          import alignment._
          import fastpMetrics.metrics._

          val totalReads = alignment.pairMetrics.totalReads

          f"$project%-14s$sampleId%-14s$lane%-7s$baitSet%-15s${totalReads / 1E6}%10.2fMb$meanTargetCoverage%13.1fx$pctPfReads%6.2f%%$pctPfReadsAligned%13.2f%%$pctPfUniqueReadsAligned%15.2f%%$pctDuplication%6.2f%%${readPairDuplicates / 1E6}%7.2fMb${readPairOpticalDuplicates / 1E6}%8.2fMb$badCycles%10s$pctChimeras%8.2f%%$pctTargetBases10%11.2f%%$pctTargetBases30%11.2f%%$pctTargetBases50%11.2f%%$gcContent%6.2f$insertSizePeak%15s"

      }
      .mkString("\n")

    header + "\n" + lines

  }

  val runQCTable =
    AsyncTask[RunQCTableInput, RunQCTable]("__runqctable", 1) {
      case RunQCTableInput(runId, sampleMetrics) =>
        implicit computationEnvironment =>
          def read(f: File) = fileutils.openSource(f)(_.mkString)
          def parseAlignmentSummaries(m: SampleMetrics) =
            m.alignmentSummary.file
              .map(read)
              .map(txt =>
                AlignmentSummaryMetrics
                  .Root(txt, m.project, m.sampleId, m.runId))
          def parseFastpReport(m: SampleMetrics) =
            Future.traverse(m.fastpReports) { fpReport =>
              fpReport.json.file
                .map(read)
                .map(
                  txt =>
                    FastpReportModel.Root(txt,
                                          fpReport.project,
                                          fpReport.sampleId,
                                          fpReport.runId,
                                          fpReport.lane))
            }
          def parseHsMetrics(m: SampleMetrics) =
            m.hsMetrics.file
              .map(read)
              .map(txt => HsMetrics.Root(txt, m.project, m.sampleId, m.runId))
          def parseDupMetrics(m: SampleMetrics) =
            m.duplicationMetrics.file
              .map(read)
              .map(txt =>
                DuplicationMetrics.Root(txt, m.project, m.sampleId, m.runId))

          def parse(m: SampleMetrics) =
            for {
              alignmentSummariesPerLane <- parseAlignmentSummaries(m)
              fastpReportsPerLane <- parseFastpReport(m)
              hsMetricsPerLane <- parseHsMetrics(m)
              dupMetrics <- parseDupMetrics(m)
            } yield {
              alignmentSummariesPerLane.map { alSummaryOfLane =>
                val lane = alSummaryOfLane.lane
                val hsMetricsOfLane = hsMetricsPerLane.find(_.lane == lane).get
                val fastpReportOfLane =
                  fastpReportsPerLane.find(_.lane == lane).get
                (alSummaryOfLane,
                 hsMetricsOfLane,
                 dupMetrics,
                 fastpReportOfLane)
              }
            }
          type MetricsTuple = (AlignmentSummaryMetrics.Root,
                               HsMetrics.Root,
                               DuplicationMetrics.Root,
                               FastpReportModel.Root)
          val parsedFiles: Future[Seq[MetricsTuple]] =
            Future.traverse(sampleMetrics)(parse).map(_.flatten)

          for {
            metrics <- parsedFiles
            result <- {
              val table = makeTable(metrics)
              println(table)
              SharedFile(Source.single(ByteString(table.getBytes("UTF-8"))),
                         runId + ".qc.table").map(RunQCTable(_))
            }
          } yield result

    }

  val hybridizationSelection =
    AsyncTask[SelectionQCInput, SelectionQCResult]("__alignmentqc-selection", 1) {
      case SelectionQCInput(coordinateSortedBam, reference, bedFile) =>
        implicit computationEnvironment =>
          for {
            reference <- reference.localFile
            bam <- coordinateSortedBam.localFile
            bed <- bedFile.file.file
            result <- {

              val picardJar = BWAAlignment.extractPicardJar()
              val maxHeap = s"-Xmx${resourceAllocated.memory}m"
              val tmpOut = TempFile.createTempFile(".qc")
              val picardStyleInterval = TempFile.createTempFile("")

              Exec.bash(logDiscriminator = "qc.bam.selection",
                        onError = Exec.ThrowIfNonZero)(s""" \\
        java -Xmx2G -Dpicard.useLegacyParser=false -jar $picardJar BedToIntervalList \\
          --INPUT ${bed.getAbsolutePath} \\
          --SEQUENCE_DICTIONARY ${reference.getAbsolutePath} \\
          --OUTPUT ${picardStyleInterval.getAbsolutePath} \\
        """)

              val bashScript = s""" \\
        java $maxHeap -Dpicard.useLegacyParser=false -jar $picardJar CollectHsMetrics \\
          --INPUT ${bam.getAbsolutePath} \\
          --REFERENCE_SEQUENCE ${reference.getAbsolutePath} \\
          --OUTPUT ${tmpOut.getAbsolutePath} \\
          --BAIT_INTERVALS ${picardStyleInterval.getAbsolutePath} \\
          --TARGET_INTERVALS ${picardStyleInterval.getAbsolutePath} \\
          --METRIC_ACCUMULATION_LEVEL "READ_GROUP" \\
          --BAIT_SET_NAME ${bedFile.file.name} \\
        """

              Exec.bash(logDiscriminator = "qc.bam.selection",
                        onError = Exec.ThrowIfNonZero)(bashScript)

              for {
                metricsFile <- SharedFile(
                  tmpOut,
                  coordinateSortedBam.bam.name + ".hsMetrics")
              } yield SelectionQCResult(metricsFile)

            }
          } yield result
    }

  val general =
    AsyncTask[AlignmentQCInput, AlignmentQCResult]("__alignmentqc-general", 1) {
      case AlignmentQCInput(coordinateSortedBam, reference) =>
        implicit computationEnvironment =>
          for {
            reference <- reference.localFile
            bam <- coordinateSortedBam.localFile
            result <- {
              val picardJar = BWAAlignment.extractPicardJar()
              val maxHeap = s"-Xmx${resourceAllocated.memory}m"
              val tmpOut = TempFile.createTempFile(".qc")
              val bashScript = s""" \\
        java $maxHeap -Dpicard.useLegacyParser=false -jar $picardJar CollectMultipleMetrics \\
          --INPUT ${bam.getAbsolutePath} \\
          --REFERENCE_SEQUENCE=${reference.getAbsolutePath} \\
          --OUTPUT ${tmpOut.getAbsolutePath} \\
          --ASSUME_SORTED true \\
          --PROGRAM "null" \\
          --PROGRAM "CollectAlignmentSummaryMetrics" \\
          --PROGRAM "CollectSequencingArtifactMetrics" \\
          --METRIC_ACCUMULATION_LEVEL "READ_GROUP" \\
        """

              Exec.bash(logDiscriminator = "qc.bam.general",
                        onError = Exec.ThrowIfNonZero)(bashScript)

              val stub = tmpOut.getAbsolutePath
              def outF(suffix: String) =
                SharedFile(new File(stub + suffix),
                           coordinateSortedBam.bam.name + suffix)

              for {
                alignmentSummary <- outF(".alignment_summary_metrics")
                biasDetail <- outF(".bait_bias_detail_metrics")
                biasSummary <- outF(".bait_bias_summary_metrics")
                errorSummary <- outF(".error_summary_metrics")
                preAdapterDetail <- outF(".pre_adapter_detail_metrics")
                preAdapterSummary <- outF(".pre_adapter_summary_metrics")
              } yield
                AlignmentQCResult(
                  alignmentSummary,
                  biasDetail,
                  biasSummary,
                  errorSummary,
                  preAdapterDetail,
                  preAdapterSummary
                )

            }
          } yield result
    }

}

object AlignmentQCInput {
  implicit val encoder: Encoder[AlignmentQCInput] =
    deriveEncoder[AlignmentQCInput]
  implicit val decoder: Decoder[AlignmentQCInput] =
    deriveDecoder[AlignmentQCInput]
}

object AlignmentQCResult {
  implicit val encoder: Encoder[AlignmentQCResult] =
    deriveEncoder[AlignmentQCResult]
  implicit val decoder: Decoder[AlignmentQCResult] =
    deriveDecoder[AlignmentQCResult]
}

object SelectionQCInput {
  implicit val encoder: Encoder[SelectionQCInput] =
    deriveEncoder[SelectionQCInput]
  implicit val decoder: Decoder[SelectionQCInput] =
    deriveDecoder[SelectionQCInput]
}

object SelectionQCResult {
  implicit val encoder: Encoder[SelectionQCResult] =
    deriveEncoder[SelectionQCResult]
  implicit val decoder: Decoder[SelectionQCResult] =
    deriveDecoder[SelectionQCResult]
}

object RunQCTableInput {
  implicit val encoder: Encoder[RunQCTableInput] =
    deriveEncoder[RunQCTableInput]
  implicit val decoder: Decoder[RunQCTableInput] =
    deriveDecoder[RunQCTableInput]
}

object RunQCTable {
  implicit val encoder: Encoder[RunQCTable] =
    deriveEncoder[RunQCTable]
  implicit val decoder: Decoder[RunQCTable] =
    deriveDecoder[RunQCTable]
}

object SampleMetrics {
  implicit val encoder: Encoder[SampleMetrics] =
    deriveEncoder[SampleMetrics]
  implicit val decoder: Decoder[SampleMetrics] =
    deriveDecoder[SampleMetrics]
}
