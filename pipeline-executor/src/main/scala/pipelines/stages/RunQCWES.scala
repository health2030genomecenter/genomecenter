package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import fileutils.TempFile
import org.gc.pipelines.util.Exec
import org.gc.pipelines.model._
import org.gc.pipelines.util.{Html, BAM, JVM, StableSet, Csv}
import org.gc.pipelines.model.{FastpReport => FastpReportModel}
import java.io.File
import akka.stream.scaladsl.Source
import akka.util.ByteString
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import Executables.{picardJar, fakeRscript}

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

case class SampleMetrics(analysisId: AnalysisId,
                         alignmentSummary: SharedFile,
                         hsMetrics: SharedFile,
                         duplicationMetrics: SharedFile,
                         fastpReport: FastpReport,
                         wgsMetrics: SharedFile,
                         gvcfQCIntervalMetrics: Option[SharedFile],
                         gvcfQCOverallMetrics: Option[SharedFile],
                         project: Project,
                         sampleId: SampleId,
                         insertSizeMetrics: SharedFile)
    extends WithSharedFiles(List(
      alignmentSummary,
      hsMetrics,
      duplicationMetrics,
      wgsMetrics,
      fastpReport.json,
      insertSizeMetrics) ++ gvcfQCIntervalMetrics.toList ++ gvcfQCOverallMetrics.toList: _*)

case class RunQCTableInput(fileName: String,
                           samples: StableSet[SampleMetrics],
                           rnaSeqAnalyses: StableSet[(AnalysisId, StarResult)])
    extends WithSharedFiles(samples.toSeq.flatMap(_.files): _*)

case class RunQCTable(htmlTable: SharedFile,
                      rnaCsvTable: SharedFile,
                      csvTable: SharedFile)
    extends WithSharedFiles(htmlTable, rnaCsvTable, csvTable)

object AlignmentQC {

  def getWGSMeanCoverage(qc: CollectWholeGenomeMetricsResult,
                         project: Project,
                         sample: SampleId)(
      implicit ec: ExecutionContext,
      tsc: TaskSystemComponents): Future[Double] = {
    implicit val mat = tsc.actorMaterializer
    for {
      txt <- qc.wgsMetrics.source
        .runFold(ByteString(""))(_ ++ _)
        .map(_.utf8String)
      parsed = {
        WgsMetrics.Root(txt, project, sample).metrics.meanCoverage
      }
    } yield parsed.toDouble
  }

  def getTargetedMeanCoverage(qc: SelectionQCResult,
                              project: Project,
                              sample: SampleId)(
      implicit ec: ExecutionContext,
      tsc: TaskSystemComponents): Future[Double] = {
    implicit val mat = tsc.actorMaterializer
    for {
      txt <- qc.hsMetrics.source
        .runFold(ByteString(""))(_ ++ _)
        .map(_.utf8String)
      parsed = {
        HsMetrics
          .Root(txt, project, sample)
          .map(_.metrics.meanTargetCoverage)
          .sum
      }
    } yield parsed
  }

  def makeCsvTable(
      laneMetrics: Seq[
        (AlignmentSummaryMetrics.Root, HsMetrics.Root, AnalysisId)],
      sampleMetrics: Seq[
        (DuplicationMetrics.Root,
         FastpReportModel.Root,
         WgsMetrics.Root,
         Option[VariantCallingMetrics.Root],
         Option[VariantCallingMetrics.Root],
         InsertSizeMetrics.Root,
         AnalysisId)],
      rnaSeqMetrics: Seq[(AnalysisId, StarMetrics.Root)]): String = {

    val left = true
    val right = false
    val sampleLines = sampleMetrics
      .sortBy(_._1.project.toString)
      .sortBy(_._1.sampleId.toString)
      .map {
        case (dups,
              fastpMetrics,
              wgsMetrics,
              mayVcfIntervalMetrics,
              mayVcfOverallMetrics,
              insertSizeMetrics,
              analysisId) =>
          import dups.metrics._
          import dups._
          import fastpMetrics.metrics._
          import wgsMetrics.metrics._
          import insertSizeMetrics.metrics._

          Csv.line(
            Seq(
              project -> left,
              sampleId -> left,
              analysisId -> left,
              f"${genomeTerritory / 1E6}%10.2fMb" -> right,
              // f"${totalReads / 1E6}%10.2fM" -> right,
              f"$meanCoverage%13.1fx" -> right,
              f"${pctDuplication * 100}%6.2f%%" -> right,
              f"${readPairDuplicates / 1E6}%7.2fM" -> right,
              f"${readPairOpticalDuplicates / 1E6}%8.2fM" -> right,
              f"$gcContent%6.2f" -> right,
              modeInsertSize.toString -> right,
              f"${pctCoverage20x * 100}%11.2f%%" -> right,
              f"${pctCoverage60x * 100}%11.2f%%" -> right,
              f"$pctExcludedTotal%11.2f%%" -> right,
              f"${mayVcfIntervalMetrics.map(_.metrics.totalSnps).getOrElse("")}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.totalSnps).getOrElse("")}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.snpsInDbSnp).getOrElse("")}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.novelSnp).getOrElse("")}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.dbSnpTiTv).getOrElse(Double.NaN)}%11.2f" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.novelTiTv).getOrElse(Double.NaN)}%11.2f" -> right,
              f"${mayVcfIntervalMetrics.map(_.metrics.totalIndel).getOrElse(Double.NaN)}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.totalIndel).getOrElse(Double.NaN)}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.indelsInDbSnp).getOrElse(Double.NaN)}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.novelIndel).getOrElse(Double.NaN)}%s" -> right,
              "Table" -> right
            ))

      }
      .mkString("\n")

    val laneLines = laneMetrics
      .sortBy(_._1.project.toString)
      .sortBy(_._1.sampleId.toString)
      .sortBy(_._1.runId.toString)
      .sortBy(_._1.lane.toInt)
      .map {
        case (alignment, targetSelection, analysisId) =>
          import alignment.pairMetrics._
          import targetSelection.metrics._
          import alignment._

          val totalReads = alignment.pairMetrics.totalReads

          val coveragePerRead = meanTargetCoverage / totalReads.toDouble

          Csv.line(
            Seq(
              project -> left,
              sampleId -> left,
              analysisId -> left,
              runId -> left,
              lane.toString -> left,
              baitSet -> left,
              f"${totalReads / 1E6}%10.2fM" -> right,
              f"$meanTargetCoverage%13.1fx" -> right,
              f"$meanTargetCoverageIncludingDuplicates%13.1fx" -> right,
              f"${pctPfReads * 100}%6.2f%%" -> right,
              f"${pctPfReadsAligned * 100}%13.2f%%" -> right,
              f"${pctUsableBasesOnTarget * 100}%13.2f%%" -> right,
              f"${pctPfUniqueReadsAligned * 100}%15.2f%%" -> right,
              badCycles.toString -> right,
              f"${pctChimeras * 100}%8.2f%%" -> right,
              f"${pctTargetBases20 * 100}%11.2f%%" -> right,
              f"${pctTargetBases30 * 100}%11.2f%%" -> right,
              f"${pctTargetBases50 * 100}%11.2f%%" -> right,
              f"${coveragePerRead * 1E6}%11.3f" -> right,
              f"${pctExcDupe * 100}%11.2f%%" -> right,
              f"${pctExcMapQ * 100}%11.2f%%" -> right,
              f"${pctExcBaseQ * 100}%11.2f%%" -> right,
              f"${pctExcOverlap * 100}%11.2f%%" -> right,
              f"${pctExcOffTarget * 100}%11.2f%%" -> right,
              "wxs-perlane" -> right
            ))

      }
      .mkString("\n")

    val laneHeader = Csv.mkHeader(
      List("Proj", "Sample", "Analysis", "Run", "Lane", "CaptureKit"),
      List(
        "TotalReads" -> right,
        "MeanTargetCoverage" -> right,
        "MeanTargetCoverageDupeIncl" -> right,
        "PFReads" -> right,
        "PFReadsAligned" -> right,
        "OnTargetUsableBases" -> right,
        "PFUniqueReadsAligned" -> right,
        "BadCycles" -> right,
        "Chimera" -> right,
        "TargetBase20" -> right,
        "TargetBase30" -> right,
        "TargetBase50" -> right,
        "CoveragePerMillionRead" -> right,
        "ExclDupe" -> right,
        "ExclMapQ" -> right,
        "ExclBaseQ" -> right,
        "ExclOverlap" -> right,
        "ExclOffTarget" -> right,
        "Table" -> right
      )
    )

    val sampleHeader = Csv.mkHeader(
      List("Proj", "Sample", "Analysis"),
      List(
        "GenomeSize" -> right,
        "MeanCoverage" -> right,
        "Dup" -> right,
        "DupReadPairs" -> right,
        "OptDupReadPairs" -> right,
        "GC" -> right,
        "InsertSizePeak" -> right,
        "Wgs20x" -> right,
        "Wgs60x" -> right,
        "Excluded" -> right,
        "TotalSnpsInCapture" -> right,
        "TotalSnps" -> right,
        "DbSnpSnp" -> right,
        "NovelSnp" -> right,
        "DbSnpTi/Tv" -> right,
        "NovelTi/Tv" -> right,
        "TotalIndelInCapture" -> right,
        "TotalIndel" -> right,
        "DbpSnpIndel" -> right,
        "NovelIndel" -> right,
        "wxs-persample" -> right
      )
    )

    val rnaLines = rnaSeqMetrics
      .sortBy(_._2.project.toString)
      .sortBy(_._2.sampleId.toString)
      .map {
        case (analysisId, starMetrics) =>
          import starMetrics._
          import starMetrics.metrics._

          Csv.line(
            Seq(
              project -> left,
              sampleId -> left,
              runId -> left,
              analysisId -> left,
              f"${numberOfReads / 1E6}%10.2fM" -> right,
              f"$meanReadLength%13.2f" -> right,
              f"${uniquelyMappedReads / 1E6}%10.2fM" -> right,
              f"${uniquelyMappedPercentage * 100}%6.2f%%" -> right,
              f"${multiplyMappedReads / 1E6}%10.2fM" -> right,
              f"${multiplyMappedReadsPercentage * 100}%6.2f%%" -> right,
              "rna" -> right
            ))

      }
      .mkString("\n")

    val rnaHeader = Csv.mkHeader(
      List("Proj", "Sample", "Run", "AnalysisId"),
      List(
        "TotalReads" -> right,
        "MeanReadLength" -> right,
        "UniquelyMapped" -> right,
        "UniquelyMapped%" -> right,
        "Multimapped" -> right,
        "Multimapped%" -> right,
        "Table" -> right
      )
    )

    val rnaTable = rnaHeader + "\n" + rnaLines
    val laneTable = laneHeader + "\n" + laneLines
    val sampleTable = sampleHeader + "\n" + sampleLines

    laneTable + "\n" + sampleTable + "\n" + rnaTable

  }
  def makeHtmlTable(
      laneMetrics: Seq[
        (AlignmentSummaryMetrics.Root, HsMetrics.Root, AnalysisId)],
      sampleMetrics: Seq[
        (DuplicationMetrics.Root,
         FastpReportModel.Root,
         WgsMetrics.Root,
         Option[VariantCallingMetrics.Root],
         Option[VariantCallingMetrics.Root],
         InsertSizeMetrics.Root,
         AnalysisId)],
      rnaSeqMetrics: Seq[(AnalysisId, StarMetrics.Root)]): String = {

    val left = true
    val right = false
    val sampleLines = sampleMetrics
      .sortBy(_._1.project.toString)
      .sortBy(_._1.sampleId.toString)
      .map {
        case (dups,
              fastpMetrics,
              wgsMetrics,
              mayVcfIntervalMetrics,
              mayVcfOverallMetrics,
              insertSizeMetrics,
              analysisId) =>
          import dups.metrics._
          import dups._
          import fastpMetrics.metrics._
          import wgsMetrics.metrics._
          import insertSizeMetrics.metrics._

          Html.line(
            Seq(
              project -> left,
              sampleId -> left,
              analysisId -> left,
              f"${genomeTerritory / 1E6}%10.2fMb" -> right,
              // f"${totalReads / 1E6}%10.2fM" -> right,
              f"$meanCoverage%13.1fx" -> right,
              f"${pctDuplication * 100}%6.2f%%" -> right,
              f"${readPairDuplicates / 1E6}%7.2fM" -> right,
              f"${readPairOpticalDuplicates / 1E6}%8.2fM" -> right,
              f"$gcContent%6.2f" -> right,
              modeInsertSize.toString -> right,
              f"${pctCoverage20x * 100}%11.2f%%" -> right,
              f"${pctCoverage60x * 100}%11.2f%%" -> right,
              f"$pctExcludedTotal%11.2f%%" -> right,
              f"${mayVcfIntervalMetrics.map(_.metrics.totalSnps).getOrElse("")}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.totalSnps).getOrElse("")}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.snpsInDbSnp).getOrElse("")}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.novelSnp).getOrElse("")}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.dbSnpTiTv).getOrElse(Double.NaN)}%11.2f" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.novelTiTv).getOrElse(Double.NaN)}%11.2f" -> right,
              f"${mayVcfIntervalMetrics.map(_.metrics.totalIndel).getOrElse(Double.NaN)}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.totalIndel).getOrElse(Double.NaN)}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.indelsInDbSnp).getOrElse(Double.NaN)}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.novelIndel).getOrElse(Double.NaN)}%s" -> right
            ))

      }
      .mkString("\n")

    val laneLines = laneMetrics
      .sortBy(_._1.project.toString)
      .sortBy(_._1.sampleId.toString)
      .sortBy(_._1.runId.toString)
      .sortBy(_._1.lane.toInt)
      .map {
        case (alignment, targetSelection, analysisId) =>
          import alignment.pairMetrics._
          import targetSelection.metrics._
          import alignment._

          val totalReads = alignment.pairMetrics.totalReads

          val coveragePerRead = meanTargetCoverage / totalReads.toDouble

          Html.line(
            Seq(
              project -> left,
              sampleId -> left,
              analysisId -> left,
              runId -> left,
              lane.toString -> left,
              baitSet -> left,
              f"${totalReads / 1E6}%10.2fM" -> right,
              f"$meanTargetCoverage%13.1fx" -> right,
              f"$meanTargetCoverageIncludingDuplicates%13.1fx" -> right,
              f"${pctPfReads * 100}%6.2f%%" -> right,
              f"${pctPfReadsAligned * 100}%13.2f%%" -> right,
              f"${pctUsableBasesOnTarget * 100}%13.2f%%" -> right,
              f"${pctPfUniqueReadsAligned * 100}%15.2f%%" -> right,
              badCycles.toString -> right,
              f"${pctChimeras * 100}%8.2f%%" -> right,
              f"${pctTargetBases20 * 100}%11.2f%%" -> right,
              f"${pctTargetBases30 * 100}%11.2f%%" -> right,
              f"${pctTargetBases50 * 100}%11.2f%%" -> right,
              f"${coveragePerRead * 1E6}%11.3f" -> right,
              f"${pctExcDupe * 100}%11.2f%%" -> right,
              f"${pctExcMapQ * 100}%11.2f%%" -> right,
              f"${pctExcBaseQ * 100}%11.2f%%" -> right,
              f"${pctExcOverlap * 100}%11.2f%%" -> right,
              f"${pctExcOffTarget * 100}%11.2f%%" -> right,
            ))

      }
      .mkString("\n")

    val laneHeader = Html.mkHeader(
      List("Proj", "Sample", "Analysis", "Run", "Lane", "CaptureKit"),
      List(
        "TotalReads" -> right,
        "MeanTargetCoverage" -> right,
        "MeanTargetCoverageDupeIncl" -> right,
        "PFReads" -> right,
        "PFReadsAligned" -> right,
        "OnTargetUsableBases" -> right,
        "PFUniqueReadsAligned" -> right,
        "BadCycles" -> right,
        "Chimera" -> right,
        "TargetBase20" -> right,
        "TargetBase30" -> right,
        "TargetBase50" -> right,
        "CoveragePerMillionRead" -> right,
        "ExclDupe" -> right,
        "ExclMapQ" -> right,
        "ExclBaseQ" -> right,
        "ExclOverlap" -> right,
        "ExclOffTarget" -> right,
      )
    )

    val sampleHeader = Html.mkHeader(
      List("Proj", "Sample", "Analysis"),
      List(
        "GenomeSize" -> right,
        "MeanCoverage" -> right,
        "Dup" -> right,
        "DupReadPairs" -> right,
        "OptDupReadPairs" -> right,
        "GC" -> right,
        "InsertSizePeak" -> right,
        "Wgs20x" -> right,
        "Wgs60x" -> right,
        "Excluded" -> right,
        "TotalSnpsInCapture" -> right,
        "TotalSnps" -> right,
        "DbSnpSnp" -> right,
        "NovelSnp" -> right,
        "DbSnpTi/Tv" -> right,
        "NovelTi/Tv" -> right,
        "TotalIndelInCapture" -> right,
        "TotalIndel" -> right,
        "DbpSnpIndel" -> right,
        "NovelIndel" -> right
      )
    )

    val rnaLines = rnaSeqMetrics
      .sortBy(_._2.project.toString)
      .sortBy(_._2.sampleId.toString)
      .map {
        case (analysisId, starMetrics) =>
          import starMetrics._
          import starMetrics.metrics._

          Html.line(
            Seq(
              project -> left,
              sampleId -> left,
              runId -> left,
              analysisId -> left,
              f"${numberOfReads / 1E6}%10.2fM" -> right,
              f"$meanReadLength%13.2f" -> right,
              f"${uniquelyMappedReads / 1E6}%10.2fM" -> right,
              f"${uniquelyMappedPercentage * 100}%6.2f%%" -> right,
              f"${multiplyMappedReads / 1E6}%10.2fM" -> right,
              f"${multiplyMappedReadsPercentage * 100}%6.2f%%" -> right
            ))

      }
      .mkString("\n")

    val rnaHeader = Html.mkHeader(
      List("Proj", "Sample", "Run", "AnalysisId"),
      List(
        "TotalReads" -> right,
        "MeanReadLength" -> right,
        "UniquelyMapped" -> right,
        "UniquelyMapped%" -> right,
        "Multimapped" -> right,
        "Multimapped%" -> right
      )
    )

    val rnaTable = """<table style="border-collapse: collapse;">""" + rnaHeader + "\n<tbody>" + rnaLines + "</tbody></table>"
    val laneTable = """<table style="border-collapse: collapse;">""" + laneHeader + "\n<tbody>" + laneLines + "</tbody></table>"
    val sampleTable = """<table style="border-collapse: collapse;">""" + sampleHeader + "\n<tbody>" + sampleLines + "</tbody></table>"

    """<!DOCTYPE html><head></head><body>""" + laneTable + sampleTable + rnaTable + "</body>"

  }
  def makeNarrowHtmlTable(
      laneMetrics: Seq[
        (AlignmentSummaryMetrics.Root, HsMetrics.Root, AnalysisId)],
      sampleMetrics: Seq[
        (DuplicationMetrics.Root,
         FastpReportModel.Root,
         WgsMetrics.Root,
         Option[VariantCallingMetrics.Root],
         Option[VariantCallingMetrics.Root],
         InsertSizeMetrics.Root,
         AnalysisId)],
      rnaSeqMetrics: Seq[(AnalysisId, StarMetrics.Root)]): String = {

    val left = true
    val right = false

    case class AggregatedLaneMetrics(
        totalReads: Long,
        totalPfReads: Long,
        totalPfReadsAligned: Long,
        totalPfNonDupReads: Long,
        totalPfNonDupReadsAligned: Long,
        totalMeanTargetCoverage: Double,
        totalMeanTargetCoverageIncludingDuplicates: Double,
        totalCoveragePerRead: Double,
        totalPercentPfNonDupReadsAligned: Double,
        totalPercentPfReadsAligned: Double
    )

    val aggregatedLanesPerSample = laneMetrics
      .groupBy(v => (v._1.project, v._1.sampleId))
      .map {
        case (projectAndSample, group) =>
          val totalReads = group.map {
            case (alignment, _, _) => alignment.pairMetrics.totalReads
          }.sum
          val totalMeanTargetCoverage = group.map {
            case (_, targetSelection, _) =>
              targetSelection.metrics.meanTargetCoverage
          }.sum
          val totalMeanTargetCoverageIncludingDuplicates = group.map {
            case (_, targetSelection, _) =>
              targetSelection.metrics.meanTargetCoverageIncludingDuplicates
          }.sum

          val totalCoveragePerRead = totalMeanTargetCoverage / totalReads.toDouble

          val totalPfReadsAligned = group.map {
            case (alignment, _, _) => alignment.pairMetrics.pfReadsAligned
          }.sum

          val totalPfReads = group.map {
            case (alignment, _, _) => alignment.pairMetrics.pfReads
          }.sum

          val totalPercentPfReadsAligned = totalPfReadsAligned.toDouble / totalPfReads.toDouble

          val totalPfUniqueReads = group.map {
            case (_, targetSelection, _) =>
              targetSelection.metrics.pfUniqueReads
          }.sum
          val totalPfUniqueReadsAligned = group.map {
            case (_, targetSelection, _) =>
              targetSelection.metrics.pfUniqueReadsAligned
          }.sum
          val totalPercentPfUniqueReadsAligned = totalPfUniqueReads.toDouble / totalPfUniqueReadsAligned
            .toDouble

          (projectAndSample,
           AggregatedLaneMetrics(
             totalReads = totalReads,
             totalPfReads = totalPfReads,
             totalPfReadsAligned = totalPfReadsAligned,
             totalPfNonDupReads = totalPfUniqueReads,
             totalPfNonDupReadsAligned = totalPfUniqueReadsAligned,
             totalMeanTargetCoverage = totalMeanTargetCoverage,
             totalMeanTargetCoverageIncludingDuplicates =
               totalMeanTargetCoverageIncludingDuplicates,
             totalCoveragePerRead = totalCoveragePerRead,
             totalPercentPfNonDupReadsAligned = totalPercentPfUniqueReadsAligned,
             totalPercentPfReadsAligned = totalPercentPfReadsAligned
           ))

      }

    val sampleLines = sampleMetrics
      .sortBy(_._1.project.toString)
      .sortBy(_._1.sampleId.toString)
      .map {
        case (dups,
              _,
              wgsMetrics,
              mayVcfIntervalMetrics,
              mayVcfOverallMetrics,
              insertSizeMetrics,
              _) =>
          import dups.metrics._
          import dups._
          import wgsMetrics.metrics._
          import insertSizeMetrics.metrics._

          val aggregatedLaneMetrics =
            aggregatedLanesPerSample((project, sampleId))

          Html.line(
            Seq(
              project -> left,
              sampleId -> left,
              f"$meanCoverage%13.1fx" -> right,
              f"${aggregatedLaneMetrics.totalMeanTargetCoverage}%13.1fx" -> right,
              f"${aggregatedLaneMetrics.totalMeanTargetCoverageIncludingDuplicates}%13.1fx" -> right,
              f"${aggregatedLaneMetrics.totalReads / 1E6}%10.2fM" -> right,
              f"${aggregatedLaneMetrics.totalPfReads / 1E6}%10.2fM" -> right,
              f"${aggregatedLaneMetrics.totalPercentPfReadsAligned * 100}%6.2f%%" -> right,
              f"${aggregatedLaneMetrics.totalPfNonDupReads / 1E6}%10.2fM" -> right,
              f"${aggregatedLaneMetrics.totalPfNonDupReadsAligned / 1E6}%10.2fM" -> right,
              f"${aggregatedLaneMetrics.totalPercentPfNonDupReadsAligned * 100}%6.2f%%" -> right,
              f"${pctDuplication * 100}%6.2f%%" -> right,
              f"${readPairDuplicates / 1E6}%7.2fM" -> right,
              f"${readPairOpticalDuplicates / 1E6}%8.2fM" -> right,
              modeInsertSize.toString -> right,
              f"${pctCoverage20x * 100}%11.2f%%" -> right,
              f"${pctCoverage60x * 100}%11.2f%%" -> right,
              f"$pctExcludedTotal%11.2f%%" -> right,
              f"${mayVcfIntervalMetrics.map(_.metrics.totalSnps).getOrElse("")}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.totalSnps).getOrElse("")}%s" -> right,
              f"${mayVcfIntervalMetrics.map(_.metrics.totalIndel).getOrElse(Double.NaN)}%s" -> right,
              f"${mayVcfOverallMetrics.map(_.metrics.totalIndel).getOrElse(Double.NaN)}%s" -> right
            ))

      }
      .mkString("\n")

    val laneLines = laneMetrics
      .sortBy(_._1.project.toString)
      .sortBy(_._1.sampleId.toString)
      .sortBy(_._1.runId.toString)
      .sortBy(_._1.lane.toInt)
      .map {
        case (alignment, targetSelection, _) =>
          import alignment.pairMetrics._
          import targetSelection.metrics._
          import alignment._

          val totalReads = alignment.pairMetrics.totalReads

          val coveragePerRead = meanTargetCoverage / totalReads.toDouble

          Html.line(
            Seq(
              project -> left,
              sampleId -> left,
              runId -> left,
              lane.toString -> left,
              f"${totalReads / 1E6}%10.2fM" -> right,
              f"$meanTargetCoverage%13.1fx" -> right,
              f"$meanTargetCoverageIncludingDuplicates%13.1fx" -> right,
              f"${pctPfReads * 100}%6.2f%%" -> right,
              f"${pctPfReadsAligned * 100}%13.2f%%" -> right,
              f"${pctUsableBasesOnTarget * 100}%13.2f%%" -> right,
              f"${pctPfUniqueReadsAligned * 100}%15.2f%%" -> right,
              badCycles.toString -> right,
              f"${pctChimeras * 100}%8.2f%%" -> right,
              f"${pctTargetBases20 * 100}%11.2f%%" -> right,
              f"${pctTargetBases30 * 100}%11.2f%%" -> right,
              f"${pctTargetBases50 * 100}%11.2f%%" -> right,
              f"${coveragePerRead * 1E6}%11.3f" -> right,
              f"${pctExcDupe * 100}%11.2f%%" -> right,
              f"${pctExcMapQ * 100}%11.2f%%" -> right,
              f"${pctExcBaseQ * 100}%11.2f%%" -> right,
              f"${pctExcOverlap * 100}%11.2f%%" -> right,
              f"${pctExcOffTarget * 100}%11.2f%%" -> right,
            ))

      }
      .mkString("\n")

    val laneHeader = Html.mkHeader(
      List("Proj", "Sample", "Run", "Lane"),
      List(
        "TotalReads" -> right,
        "MeanTargetCoverage" -> right,
        "MeanTargetCoverageDupeIncl" -> right,
        "PFReads" -> right,
        "PFReadsAligned" -> right,
        "OnTargetUsableBases" -> right,
        "PFUniqueReadsAligned" -> right,
        "BadCycles" -> right,
        "Chimera" -> right,
        "TargetBase20" -> right,
        "TargetBase30" -> right,
        "TargetBase50" -> right,
        "CoveragePerMillionRead" -> right,
        "ExclDupe" -> right,
        "ExclMapQ" -> right,
        "ExclBaseQ" -> right,
        "ExclOverlap" -> right,
        "ExclOffTarget" -> right,
      )
    )

    val sampleHeader = Html.mkHeader(
      List("Proj", "Sample"),
      List(
        "MeanWgsCoverage" -> right,
        "MeanTargetCoverage" -> right,
        "MeanTargetCoverageDupIncl" -> right,
        "TotalReads" -> right,
        "PFReads" -> right,
        "PFReadsAligned" -> right,
        "PFUniqueReads" -> right,
        "PFUniqueReadsAligned" -> right,
        "Dup" -> right,
        "DupReadPairs" -> right,
        "OptDupReadPairs" -> right,
        "InsertSizePeak" -> right,
        "Wgs20x" -> right,
        "Wgs60x" -> right,
        "Excluded" -> right,
        "TotalSnpsInCapture" -> right,
        "TotalSnps" -> right,
        "TotalIndelInCapture" -> right,
        "TotalIndel" -> right
      )
    )

    val rnaLines = rnaSeqMetrics
      .sortBy(_._2.project.toString)
      .sortBy(_._2.sampleId.toString)
      .map {
        case (_, starMetrics) =>
          import starMetrics._
          import starMetrics.metrics._

          Html.line(
            Seq(
              project -> left,
              sampleId -> left,
              runId -> left,
              f"${numberOfReads / 1E6}%10.2fM" -> right,
              f"$meanReadLength%13.2f" -> right,
              f"${uniquelyMappedReads / 1E6}%10.2fM" -> right,
              f"${uniquelyMappedPercentage * 100}%6.2f%%" -> right,
              f"${multiplyMappedReads / 1E6}%10.2fM" -> right,
              f"${multiplyMappedReadsPercentage * 100}%6.2f%%" -> right
            ))

      }
      .mkString("\n")

    val rnaHeader = Html.mkHeader(
      List("Proj", "Sample", "Run"),
      List(
        "TotalReads" -> right,
        "MeanReadLength" -> right,
        "UniquelyMapped" -> right,
        "UniquelyMapped%" -> right,
        "Multimapped" -> right,
        "Multimapped%" -> right
      )
    )

    val rnaTable = """<table style="border-collapse: collapse;">""" + rnaHeader + "\n<tbody>" + rnaLines + "</tbody></table>"
    val laneTable = """<table style="border-collapse: collapse;">""" + laneHeader + "\n<tbody>" + laneLines + "</tbody></table>"
    val sampleTable = """<table style="border-collapse: collapse;">""" + sampleHeader + "\n<tbody>" + sampleLines + "</tbody></table>"

    """<!DOCTYPE html><head></head><body>""" + sampleTable + laneTable + rnaTable + "</body>"

  }

  val runQCTable =
    AsyncTask[RunQCTableInput, RunQCTable]("__runqctable", 3) {
      case RunQCTableInput(fileName, sampleMetrics, rnaAnalyses) =>
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

          def parseVariantCallingMetrics(m: Option[SharedFile],
                                         project: Project,
                                         sampleId: SampleId) =
            m match {
              case None => Future.successful(None)
              case Some(file) =>
                file.file
                  .map(read)
                  .map(txt =>
                    Some(VariantCallingMetrics.Root(txt, project, sampleId)))
            }

          def parse(m: SampleMetrics) =
            for {
              alignmentSummariesPerLanePerRun <- parseAlignmentSummaries(m)
              fastpReport <- parseFastpReport(m)
              hsMetricsPerLanePerRun <- parseHsMetrics(m)
              dupMetrics <- parseDupMetrics(m)
              wgsMetrics <- parseWgsMetrics(m)
              variantQCIntervalMetrics <- parseVariantCallingMetrics(
                m.gvcfQCIntervalMetrics,
                m.project,
                m.sampleId)
              variantQCOverallMetrics <- parseVariantCallingMetrics(
                m.gvcfQCOverallMetrics,
                m.project,
                m.sampleId)
              insertSizeMetrics <- parseInsertSizeMetrics(m)
            } yield {
              val laneSpecificMetrics = alignmentSummariesPerLanePerRun.map {
                alSummaryOfLane =>
                  val hsMetricsOfLane = hsMetricsPerLanePerRun
                    .find(hs =>
                      hs.lane == alSummaryOfLane.lane && hs.runId == alSummaryOfLane.runId)
                    .get
                  (alSummaryOfLane, hsMetricsOfLane, m.analysisId)
              }

              val sampleSpecificMetrics = (dupMetrics,
                                           fastpReport,
                                           wgsMetrics,
                                           variantQCIntervalMetrics,
                                           variantQCOverallMetrics,
                                           insertSizeMetrics,
                                           m.analysisId)

              (laneSpecificMetrics, sampleSpecificMetrics)

            }

          val parsedFiles =
            Future
              .traverse(sampleMetrics.toSeq)(parse)
              .map(pair => (pair.flatMap(_._1), pair.map(_._2)))

          val parsedRNAResults = Future.traverse(rnaAnalyses.toSeq) {
            case (analysisId,
                  StarResult(log,
                             run,
                             BamWithSampleMetadata(project, sample, _))) =>
              implicit val mat =
                computationEnvironment.components.actorMaterializer
              log.source
                .runFold(ByteString.empty)(_ ++ _)
                .map(_.utf8String)
                .map { content =>
                  (analysisId, StarMetrics.Root(content, project, sample, run))
                }
          }

          for {
            (laneMetrics, sampleMetrics) <- parsedFiles
            parsedRNAMetrics <- parsedRNAResults
            csv = RunQCRNA.makeCsvTable(parsedRNAMetrics)
            rnaCSVTable <- SharedFile(
              Source.single(ByteString(csv.getBytes("UTF-8"))),
              fileName + ".star.qc.table.csv")

            htmlTable <- {
              val table =
                makeHtmlTable(laneMetrics, sampleMetrics, parsedRNAMetrics)
              SharedFile(Source.single(ByteString(table.getBytes("UTF-8"))),
                         fileName + ".wes.qc.table.html")
            }
            csvTable <- {
              val table =
                makeCsvTable(laneMetrics, sampleMetrics, parsedRNAMetrics)
              SharedFile(Source.single(ByteString(table.getBytes("UTF-8"))),
                         fileName + ".wes.qc.table.csv")
            }
            _ <- {
              val analyses
                : Seq[AnalysisId] = (laneMetrics.map(_._3) ++ sampleMetrics.map(
                _._7) ++ parsedRNAMetrics.map(_._1)).distinct
              Future.traverse(analyses) { analysis =>
                val table =
                  makeNarrowHtmlTable(laneMetrics.filter(_._3 == analysis),
                                      sampleMetrics.filter(_._7 == analysis),
                                      parsedRNAMetrics.filter(_._1 == analysis))
                SharedFile(Source.single(ByteString(table.getBytes("UTF-8"))),
                           fileName + "." + analysis + ".wes.qc.table.html")
              }
            }
          } yield RunQCTable(htmlTable, rnaCSVTable, csvTable)

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
