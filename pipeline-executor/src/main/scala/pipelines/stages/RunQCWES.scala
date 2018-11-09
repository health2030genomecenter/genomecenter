package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import fileutils.TempFile
import org.gc.pipelines.util.Exec
import org.gc.pipelines.model._
import org.gc.pipelines.util.{Html, BAM, JVM}
import org.gc.pipelines.model.{FastpReport => FastpReportModel}
import java.io.File
import scala.concurrent.Future
import akka.stream.scaladsl.Source
import akka.util.ByteString

case class AlignmentQCInput(bam: CoordinateSortedBam,
                            reference: IndexedReferenceFasta)
    extends WithSharedFiles((bam.files ++ reference.files): _*)

case class CollectWholeGenomeMetricsInput(bam: CoordinateSortedBam,
                                          reference: IndexedReferenceFasta)
    extends WithSharedFiles((bam.files ++ reference.files): _*)

case class SelectionQCInput(bam: CoordinateSortedBam,
                            reference: IndexedReferenceFasta,
                            selectionTargetIntervals: BedFile)
    extends WithSharedFiles(
      (bam.files ++ reference.files ++ selectionTargetIntervals.files): _*)

case class SelectionQCResult(hsMetrics: SharedFile)
    extends WithSharedFiles(hsMetrics)

case class CollectWholeGenomeMetricsResult(wgsMetrics: SharedFile)
    extends WithSharedFiles(wgsMetrics)

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
                         fastpReport: FastpReport,
                         wgsMetrics: SharedFile,
                         gvcfQCMetrics: SharedFile,
                         project: Project,
                         sampleId: SampleId,
                         runId: RunId)
    extends WithSharedFiles(alignmentSummary,
                            hsMetrics,
                            duplicationMetrics,
                            wgsMetrics,
                            gvcfQCMetrics,
                            fastpReport.json)

case class RunQCTableInput(runId: RunId, samples: Seq[SampleMetrics])
    extends WithSharedFiles(samples.flatMap(_.files): _*)

case class RunQCTable(table: SharedFile, htmlTable: SharedFile)
    extends WithSharedFiles(table, htmlTable)

object AlignmentQC {

  def makeHtmlTable(
      metrics: Seq[
        (AlignmentSummaryMetrics.Root,
         HsMetrics.Root,
         DuplicationMetrics.Root,
         FastpReportModel.Root,
         WgsMetrics.Root,
         VariantCallingMetrics.Root)]): String = {

    val left = true
    val right = false
    val lines = metrics
      .sortBy(_._1.project.toString)
      .sortBy(_._1.lane.toInt)
      .map {
        case (alignment,
              targetSelection,
              dups,
              fastpMetrics,
              wgsMetrics,
              vcfMetrics) =>
          import alignment.pairMetrics._
          import targetSelection.metrics._
          import dups.metrics._
          import alignment._
          import fastpMetrics.metrics._
          import wgsMetrics.metrics._
          import vcfMetrics.metrics._

          val totalReads = alignment.pairMetrics.totalReads

          Html.line(
            Seq(
              project -> left,
              sampleId -> left,
              lane.toString -> left,
              baitSet -> left,
              f"${genomeTerritory / 1E6}%10.2fMb" -> right,
              f"${totalReads / 1E6}%10.2fM" -> right,
              f"$meanTargetCoverage%13.1fx" -> right,
              f"$meanCoverage%13.1fx" -> right,
              f"${pctPfReads * 100}%6.2f%%" -> right,
              f"${pctPfReadsAligned * 100}%13.2f%%" -> right,
              f"${pctPfUniqueReadsAligned * 100}%15.2f%%" -> right,
              f"${pctDuplication * 100}%6.2f%%" -> right,
              f"${readPairDuplicates / 1E6}%7.2fM" -> right,
              f"${readPairOpticalDuplicates / 1E6}%8.2fM" -> right,
              badCycles.toString -> right,
              f"${pctChimeras * 100}%8.2f%%" -> right,
              f"${pctTargetBases10 * 100}%11.2f%%" -> right,
              f"${pctTargetBases30 * 100}%11.2f%%" -> right,
              f"${pctTargetBases50 * 100}%11.2f%%" -> right,
              f"$gcContent%6.2f" -> right,
              insertSizePeak.toString -> right,
              f"${pctCoverage10x * 100}%11.2f%%" -> right,
              f"${pctCoverage20x * 100}%11.2f%%" -> right,
              f"${pctCoverage60x * 100}%11.2f%%" -> right,
              f"$pctExcludedTotal%11.2f%%" -> right,
              f"$totalSnps%s" -> right,
              f"$snpsInDbSnp%s" -> right,
              f"$novelSnp%s" -> right,
              f"$dbSnpTiTv%11.2f%%" -> right,
              f"$totalIndel%s" -> right,
              f"$indelsInDbSnp%s" -> right,
              f"$novelIndel%s" -> right
            ))

      }
      .mkString("\n")

    val header = Html.mkHeader(
      List("Proj", "Sample", "Lane", "CaptureKit"),
      List(
        "GenomeSize" -> right,
        "TotalReads" -> right,
        "MeanTargetCoverage" -> right,
        "MeanCoverage" -> right,
        "PFReads" -> right,
        "PFReadsAligned" -> right,
        "PFUniqueReadsAligned" -> right,
        "Dup" -> right,
        "DupReads" -> right,
        "OptDupReads" -> right,
        "BadCycles" -> right,
        "Chimera" -> right,
        "TargetBase10" -> right,
        "Targetbase30" -> right,
        "TargetBase50" -> right,
        "GC" -> right,
        "InsertSizePeak" -> right,
        "Wgs10x" -> right,
        "Wgs20x" -> right,
        "Wgs60x" -> right,
        "Excluded" -> right,
        "TotalSnps" -> right,
        "DbSnpSnp" -> right,
        "NovelSnp" -> right,
        "DbSnpTi/Tv" -> right,
        "NovelTi/Tv" -> right,
        "TotalIndel" -> right,
        "DbpSnpIndel" -> right,
        "NovelIndel" -> right
      )
    )

    """<!DOCTYPE html><head></head><body><table style="border-collapse: collapse;">""" + header + "\n<tbody>" + lines + "</tbody></table></body>"

  }

  def makeTable(
      metrics: Seq[
        (AlignmentSummaryMetrics.Root,
         HsMetrics.Root,
         DuplicationMetrics.Root,
         FastpReportModel.Root,
         WgsMetrics.Root,
         VariantCallingMetrics.Root)]): String = {
    val header =
      "Proj          Sample        Lane   CptrKit              TotRds   MeanTrgtCov %PfRds %PfRdsAligned %PfUqRdsAligned   %Dup   DupRds OptDupRds BadCycles %Chimera %TrgtBase10 %TrgtBase30 %TrgtBase50    GC InsertSizePeak"

    val lines = metrics
      .map {
        case (alignment, targetSelection, dups, fastpMetrics, _, _) =>
          import alignment.pairMetrics._
          import targetSelection.metrics._
          import dups.metrics._
          import alignment._
          import fastpMetrics.metrics._

          val totalReads = alignment.pairMetrics.totalReads

          f"$project%-14s$sampleId%-14s$lane%-7s$baitSet%-15s${totalReads / 1E6}%10.2fMb$meanTargetCoverage%13.1fx${pctPfReads * 100}%6.2f%%${pctPfReadsAligned * 100}%13.2f%%${pctPfUniqueReadsAligned * 100}%15.2f%%${pctDuplication * 100}%6.2f%%${readPairDuplicates / 1E6}%7.2fMb${readPairOpticalDuplicates / 1E6}%8.2fMb$badCycles%10s$pctChimeras%8.2f%%${pctTargetBases10 * 100}%11.2f%%${pctTargetBases30 * 100}%11.2f%%${pctTargetBases50 * 100}%11.2f%%$gcContent%6.2f$insertSizePeak%15s"

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
            m.fastpReport.json.file
              .map(read)
              .map(
                txt =>
                  FastpReportModel.Root(txt,
                                        m.fastpReport.project,
                                        m.fastpReport.sampleId,
                                        m.fastpReport.runId))
          def parseHsMetrics(m: SampleMetrics) =
            m.hsMetrics.file
              .map(read)
              .map(txt => HsMetrics.Root(txt, m.project, m.sampleId, m.runId))
          def parseWgsMetrics(m: SampleMetrics) =
            m.wgsMetrics.file
              .map(read)
              .map(txt => WgsMetrics.Root(txt, m.project, m.sampleId, m.runId))
          def parseDupMetrics(m: SampleMetrics) =
            m.duplicationMetrics.file
              .map(read)
              .map(txt =>
                DuplicationMetrics.Root(txt, m.project, m.sampleId, m.runId))

          def parseVariantCallingMetrics(m: SampleMetrics) =
            m.gvcfQCMetrics.file
              .map(read)
              .map(txt =>
                VariantCallingMetrics.Root(txt, m.project, m.sampleId, m.runId))

          def parse(m: SampleMetrics) =
            for {
              alignmentSummariesPerLane <- parseAlignmentSummaries(m)
              fastpReport <- parseFastpReport(m)
              hsMetricsPerLane <- parseHsMetrics(m)
              dupMetrics <- parseDupMetrics(m)
              wgsMetrics <- parseWgsMetrics(m)
              variantQCMetrics <- parseVariantCallingMetrics(m)
            } yield {
              alignmentSummariesPerLane.map { alSummaryOfLane =>
                val lane = alSummaryOfLane.lane
                val hsMetricsOfLane = hsMetricsPerLane.find(_.lane == lane).get
                (alSummaryOfLane,
                 hsMetricsOfLane,
                 dupMetrics,
                 fastpReport,
                 wgsMetrics,
                 variantQCMetrics)
              }
            }
          type MetricsTuple = (AlignmentSummaryMetrics.Root,
                               HsMetrics.Root,
                               DuplicationMetrics.Root,
                               FastpReportModel.Root,
                               WgsMetrics.Root,
                               VariantCallingMetrics.Root)
          val parsedFiles: Future[Seq[MetricsTuple]] =
            Future.traverse(sampleMetrics)(parse).map(_.flatten)

          for {
            metrics <- parsedFiles
            table <- {
              val table = makeTable(metrics)
              SharedFile(Source.single(ByteString(table.getBytes("UTF-8"))),
                         runId + ".qc.table")
            }
            htmlTable <- {
              val table = makeHtmlTable(metrics)
              SharedFile(Source.single(ByteString(table.getBytes("UTF-8"))),
                         runId + ".qc.table.html")
            }
          } yield RunQCTable(table, htmlTable)

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
              val maxHeap = JVM.maxHeap
              val tmpOut = TempFile.createTempFile(".qc")
              val picardStyleInterval = TempFile.createTempFile("")

              Exec.bash(logDiscriminator = "qc.bam.selection",
                        onError = Exec.ThrowIfNonZero)(s""" \\
        java ${JVM.serial} -Xmx2G -Dpicard.useLegacyParser=false -jar $picardJar BedToIntervalList \\
          --INPUT ${bed.getAbsolutePath} \\
          --SEQUENCE_DICTIONARY ${reference.getAbsolutePath} \\
          --OUTPUT ${picardStyleInterval.getAbsolutePath} \\
        """)

              val bashScript = s""" \\
        java ${JVM.g1} $maxHeap -Dpicard.useLegacyParser=false -jar $picardJar CollectHsMetrics \\
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
              val maxHeap = JVM.maxHeap
              val tmpOut = TempFile.createTempFile(".qc")
              val bashScript = s""" \\
        java ${JVM.g1} $maxHeap -Dpicard.useLegacyParser=false -jar $picardJar CollectMultipleMetrics \\
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
                           coordinateSortedBam.bam.name + suffix,
                           deleteFile = true)

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

  val wholeGenomeMetrics =
    AsyncTask[CollectWholeGenomeMetricsInput, CollectWholeGenomeMetricsResult](
      "__alignmentqc-wgs",
      1) {
      case CollectWholeGenomeMetricsInput(coordinateSortedBam, reference) =>
        implicit computationEnvironment =>
          for {
            reference <- reference.localFile
            bam <- coordinateSortedBam.localFile
            result <- {

              val readLength = BAM.getMeanReadLength(bam, 10000).toInt
              val picardJar = BWAAlignment.extractPicardJar()
              val maxHeap = JVM.maxHeap
              val tmpOut = TempFile.createTempFile(".qc")
              val bashScript = s""" \\
        java ${JVM.g1} $maxHeap -Dpicard.useLegacyParser=false -jar $picardJar CollectWgsMetrics \\
          --INPUT ${bam.getAbsolutePath} \\
          --REFERENCE_SEQUENCE=${reference.getAbsolutePath} \\
          --OUTPUT ${tmpOut.getAbsolutePath} \\
          --COUNT_UNPAIRED true \\
          --USE_FAST_ALGORITHM  true \\
          --READ_LENGTH $readLength \\
        """

              Exec.bash(logDiscriminator = "qc.bam.wgs",
                        onError = Exec.ThrowIfNonZero)(bashScript)

              for {
                metrics <- SharedFile(
                  tmpOut,
                  coordinateSortedBam.bam.name + ".wgsmetrics",
                  true)
              } yield CollectWholeGenomeMetricsResult(metrics)

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

object CollectWholeGenomeMetricsInput {
  implicit val encoder: Encoder[CollectWholeGenomeMetricsInput] =
    deriveEncoder[CollectWholeGenomeMetricsInput]
  implicit val decoder: Decoder[CollectWholeGenomeMetricsInput] =
    deriveDecoder[CollectWholeGenomeMetricsInput]
}

object CollectWholeGenomeMetricsResult {
  implicit val encoder: Encoder[CollectWholeGenomeMetricsResult] =
    deriveEncoder[CollectWholeGenomeMetricsResult]
  implicit val decoder: Decoder[CollectWholeGenomeMetricsResult] =
    deriveDecoder[CollectWholeGenomeMetricsResult]
}
