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
    preAdapterSummary: SharedFile,
    insertSizeMetrics: SharedFile
) extends WithSharedFiles(alignmentSummary,
                            biasDetail,
                            biasSummary,
                            errorSummary,
                            preAdapterDetail,
                            preAdapterSummary,
                            insertSizeMetrics)

case class SampleMetrics(alignmentSummary: SharedFile,
                         hsMetrics: SharedFile,
                         duplicationMetrics: SharedFile,
                         fastpReport: FastpReport,
                         wgsMetrics: SharedFile,
                         gvcfQCMetrics: SharedFile,
                         project: Project,
                         sampleId: SampleId,
                         insertSizeMetrics: SharedFile)
    extends WithSharedFiles(alignmentSummary,
                            hsMetrics,
                            duplicationMetrics,
                            wgsMetrics,
                            gvcfQCMetrics,
                            fastpReport.json,
                            insertSizeMetrics)

case class RunQCTableInput(fileName: String, samples: Seq[SampleMetrics])
    extends WithSharedFiles(samples.flatMap(_.files): _*)

case class RunQCTable(htmlTable: SharedFile) extends WithSharedFiles(htmlTable)

object AlignmentQC {

  def makeHtmlTable(
      laneMetrics: Seq[(AlignmentSummaryMetrics.Root, HsMetrics.Root)],
      sampleMetrics: Seq[
        (DuplicationMetrics.Root,
         FastpReportModel.Root,
         WgsMetrics.Root,
         VariantCallingMetrics.Root,
         InsertSizeMetrics.Root)]): String = {

    val left = true
    val right = false
    val sampleLines = sampleMetrics
      .sortBy(_._1.project.toString)
      .sortBy(_._1.sampleId.toString)
      .map {
        case (dups, fastpMetrics, wgsMetrics, vcfMetrics, insertSizeMetrics) =>
          import dups.metrics._
          import dups._
          import fastpMetrics.metrics._
          import wgsMetrics.metrics._
          import vcfMetrics.metrics._
          import insertSizeMetrics.metrics._

          Html.line(
            Seq(
              project -> left,
              sampleId -> left,
              f"${genomeTerritory / 1E6}%10.2fMb" -> right,
              // f"${totalReads / 1E6}%10.2fM" -> right,
              f"$meanCoverage%13.1fx" -> right,
              f"${pctDuplication * 100}%6.2f%%" -> right,
              f"${readPairDuplicates / 1E6}%7.2fM" -> right,
              f"${readPairOpticalDuplicates / 1E6}%8.2fM" -> right,
              f"$gcContent%6.2f" -> right,
              modeInsertSize.toString -> right,
              f"${pctCoverage10x * 100}%11.2f%%" -> right,
              f"${pctCoverage20x * 100}%11.2f%%" -> right,
              f"${pctCoverage60x * 100}%11.2f%%" -> right,
              f"$pctExcludedTotal%11.2f%%" -> right,
              f"$totalSnps%s" -> right,
              f"$snpsInDbSnp%s" -> right,
              f"$novelSnp%s" -> right,
              f"$dbSnpTiTv%11.2f" -> right,
              f"$novelTiTv%11.2f" -> right,
              f"$totalIndel%s" -> right,
              f"$indelsInDbSnp%s" -> right,
              f"$novelIndel%s" -> right
            ))

      }
      .mkString("\n")

    val laneLines = laneMetrics
      .sortBy(_._1.project.toString)
      .sortBy(_._1.sampleId.toString)
      .sortBy(_._1.runId.toString)
      .sortBy(_._1.lane.toInt)
      .map {
        case (alignment, targetSelection) =>
          import alignment.pairMetrics._
          import targetSelection.metrics._
          import alignment._

          val totalReads = alignment.pairMetrics.totalReads

          Html.line(
            Seq(
              project -> left,
              sampleId -> left,
              runId -> left,
              lane.toString -> left,
              baitSet -> left,
              f"${totalReads / 1E6}%10.2fM" -> right,
              f"$meanTargetCoverage%13.1fx" -> right,
              f"${pctPfReads * 100}%6.2f%%" -> right,
              f"${pctPfReadsAligned * 100}%13.2f%%" -> right,
              f"${pctPfUniqueReadsAligned * 100}%15.2f%%" -> right,
              badCycles.toString -> right,
              f"${pctChimeras * 100}%8.2f%%" -> right,
              f"${pctTargetBases10 * 100}%11.2f%%" -> right,
              f"${pctTargetBases30 * 100}%11.2f%%" -> right,
              f"${pctTargetBases50 * 100}%11.2f%%" -> right
            ))

      }
      .mkString("\n")

    val laneHeader = Html.mkHeader(
      List("Proj", "Sample", "Run", "Lane", "CaptureKit"),
      List(
        "TotalReads" -> right,
        "MeanTargetCoverage" -> right,
        "PFReads" -> right,
        "PFReadsAligned" -> right,
        "PFUniqueReadsAligned" -> right,
        "BadCycles" -> right,
        "Chimera" -> right,
        "TargetBase10" -> right,
        "Targetbase30" -> right,
        "TargetBase50" -> right
      )
    )

    val sampleHeader = Html.mkHeader(
      List("Proj", "Sample"),
      List(
        "GenomeSize" -> right,
        // "TotalReads" -> right,
        "MeanCoverage" -> right,
        "Dup" -> right,
        "DupReadPairss" -> right,
        "OptDupReadPairs" -> right,
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

    """<!DOCTYPE html><head></head><body><table style="border-collapse: collapse;">""" + laneHeader + "\n<tbody>" + laneLines + """</tbody></table><table style="border-collapse: collapse;">""" + sampleHeader + "\n<tbody>" + sampleLines + "</tbody></table></body>"

  }

  val runQCTable =
    AsyncTask[RunQCTableInput, RunQCTable]("__runqctable", 1) {
      case RunQCTableInput(fileName, sampleMetrics) =>
        implicit computationEnvironment =>
          def read(f: File) = fileutils.openSource(f)(_.mkString)
          def parseAlignmentSummaries(m: SampleMetrics) =
            m.alignmentSummary.file
              .map(read)
              .map(txt =>
                AlignmentSummaryMetrics
                  .Root(txt, m.project, m.sampleId))
          def parseInsertSizeMetrics(m: SampleMetrics) =
            m.insertSizeMetrics.file
              .map(read)
              .map(txt =>
                InsertSizeMetrics
                  .Root(txt, m.project, m.sampleId))
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
              .map(txt => HsMetrics.Root(txt, m.project, m.sampleId))
          def parseWgsMetrics(m: SampleMetrics) =
            m.wgsMetrics.file
              .map(read)
              .map(txt => WgsMetrics.Root(txt, m.project, m.sampleId))
          def parseDupMetrics(m: SampleMetrics) =
            m.duplicationMetrics.file
              .map(read)
              .map(txt => DuplicationMetrics.Root(txt, m.project, m.sampleId))

          def parseVariantCallingMetrics(m: SampleMetrics) =
            m.gvcfQCMetrics.file
              .map(read)
              .map(txt =>
                VariantCallingMetrics.Root(txt, m.project, m.sampleId))

          def parse(m: SampleMetrics) =
            for {
              alignmentSummariesPerLanePerRun <- parseAlignmentSummaries(m)
              fastpReport <- parseFastpReport(m)
              hsMetricsPerLanePerRun <- parseHsMetrics(m)
              dupMetrics <- parseDupMetrics(m)
              wgsMetrics <- parseWgsMetrics(m)
              variantQCMetrics <- parseVariantCallingMetrics(m)
              insertSizeMetrics <- parseInsertSizeMetrics(m)
            } yield {
              val laneSpecificMetrics = alignmentSummariesPerLanePerRun.map {
                alSummaryOfLane =>
                  val hsMetricsOfLane = hsMetricsPerLanePerRun
                    .find(hs =>
                      hs.lane == alSummaryOfLane.lane && hs.runId == alSummaryOfLane.runId)
                    .get
                  (alSummaryOfLane, hsMetricsOfLane)
              }

              val sampleSpecificMetrics = (dupMetrics,
                                           fastpReport,
                                           wgsMetrics,
                                           variantQCMetrics,
                                           insertSizeMetrics)

              (laneSpecificMetrics, sampleSpecificMetrics)

            }

          val parsedFiles =
            Future
              .traverse(sampleMetrics)(parse)
              .map(pair => (pair.flatMap(_._1), pair.map(_._2)))

          for {
            (laneMetrics, sampleMetrics) <- parsedFiles

            htmlTable <- {
              val table = makeHtmlTable(laneMetrics, sampleMetrics)
              SharedFile(Source.single(ByteString(table.getBytes("UTF-8"))),
                         fileName + ".wes.qc.table.html")
            }
          } yield RunQCTable(htmlTable)

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
              val fakeRscript = BWAAlignment.extractFakeRscript()
              val maxHeap = JVM.maxHeap
              val tmpOut = TempFile.createTempFile(".qc")
              val bashScript = s""" \\
        PATH=${fakeRscript.getParentFile}:$$PATH java ${JVM.g1} $maxHeap -Dpicard.useLegacyParser=false -jar $picardJar CollectMultipleMetrics \\
          --INPUT ${bam.getAbsolutePath} \\
          --REFERENCE_SEQUENCE=${reference.getAbsolutePath} \\
          --OUTPUT ${tmpOut.getAbsolutePath} \\
          --ASSUME_SORTED true \\
          --PROGRAM "null" \\
          --PROGRAM "CollectAlignmentSummaryMetrics" \\
          --PROGRAM "CollectInsertSizeMetrics" \\
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
                insertSizeMetrics <- outF(".insert_size_metrics")
              } yield
                AlignmentQCResult(
                  alignmentSummary,
                  biasDetail,
                  biasSummary,
                  errorSummary,
                  preAdapterDetail,
                  preAdapterSummary,
                  insertSizeMetrics
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

              val readLength = BAM.getMaxReadLength(bam, 5000000).toInt
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
