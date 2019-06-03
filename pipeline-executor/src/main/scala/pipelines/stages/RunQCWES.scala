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
    extends WithMutableSharedFiles(mutables = List(hsMetrics), immutables = Nil)

case class CollectWholeGenomeMetricsResult(wgsMetrics: SharedFile)
    extends WithMutableSharedFiles(mutables = List(wgsMetrics),
                                   immutables = Nil)

case class AlignmentQCResult(
    alignmentSummary: SharedFile,
    biasDetail: SharedFile,
    biasSummary: SharedFile,
    errorSummary: SharedFile,
    preAdapterDetail: SharedFile,
    preAdapterSummary: SharedFile,
    insertSizeMetrics: SharedFile
) extends WithMutableSharedFiles(mutables = List(alignmentSummary,
                                                   biasDetail,
                                                   biasSummary,
                                                   errorSummary,
                                                   preAdapterDetail,
                                                   preAdapterSummary,
                                                   insertSizeMetrics),
                                   immutables = Nil)

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
                         insertSizeMetrics: SharedFile,
                         fastCoverages: List[(RunId, MeanCoverageResult)])
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
         AnalysisId,
         List[(RunId, MeanCoverageResult)])],
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
              analysisId,
              _) =>
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
              s"$genomeTerritory" -> right,
              s"$meanCoverage" -> right,
              s"$pctDuplication" -> right,
              s"$readPairDuplicates" -> right,
              s"$readPairOpticalDuplicates" -> right,
              s"$gcContent" -> right,
              modeInsertSize.toString -> right,
              s"$pctCoverage20x" -> right,
              s"$pctCoverage60x" -> right,
              s"$pctExcludedTotal" -> right,
              s"${mayVcfIntervalMetrics.map(_.metrics.totalSnps.toDouble).getOrElse(Double.NaN)}" -> right,
              s"${mayVcfOverallMetrics.map(_.metrics.totalSnps.toDouble).getOrElse(Double.NaN)}" -> right,
              s"${mayVcfOverallMetrics.map(_.metrics.snpsInDbSnp.toDouble).getOrElse(Double.NaN)}" -> right,
              s"${mayVcfOverallMetrics.map(_.metrics.novelSnp.toDouble).getOrElse(Double.NaN)}" -> right,
              s"${mayVcfOverallMetrics.map(_.metrics.dbSnpTiTv).getOrElse(Double.NaN)}" -> right,
              s"${mayVcfOverallMetrics.map(_.metrics.novelTiTv).getOrElse(Double.NaN)}" -> right,
              s"${mayVcfIntervalMetrics.map(_.metrics.totalIndel.toDouble).getOrElse(Double.NaN)}" -> right,
              s"${mayVcfOverallMetrics.map(_.metrics.totalIndel.toDouble).getOrElse(Double.NaN)}" -> right,
              s"${mayVcfOverallMetrics.map(_.metrics.indelsInDbSnp.toDouble).getOrElse(Double.NaN)}" -> right,
              s"${mayVcfOverallMetrics.map(_.metrics.novelIndel.toDouble).getOrElse(Double.NaN)}" -> right,
              "wxs-persample" -> right
            ))

      }
      .mkString("\n")

    val fastCoveragePerRun = sampleMetrics.flatMap { sampleMetric =>
      sampleMetric._8.map {
        case (runId, meanCoverage) =>
          (sampleMetric._1.project,
           sampleMetric._1.sampleId,
           runId,
           sampleMetric._7,
           meanCoverage)
      }
    }

    val runLines =
      fastCoveragePerRun
        .sortBy(_._2.toString)
        .sortBy(_._1.toString)
        .sortBy(_._3.toString)
        .map {
          case (sampleId, project, runId, analysisId, fastCoverage) =>
            Csv.line(
              Seq(
                project -> left,
                sampleId -> left,
                analysisId -> left,
                runId -> left,
                fastCoverage.all.toString -> right,
                fastCoverage.all.toString -> right,
                "wxs-perrun" -> right
              ))
        }
        .mkString("\n")

    val runHeader = Csv.mkHeader(
      List("Proj", "Sample", "Analysis", "Run"),
      List(
        "FastWgsCoverage" -> right,
        "FastTargetCoverage" -> right,
        "table" -> right
      )
    )

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
              s"$totalReads" -> right,
              s"$meanTargetCoverage" -> right,
              s"$meanTargetCoverageIncludingDuplicates" -> right,
              s"$pctPfReads" -> right,
              s"$pctPfReadsAligned" -> right,
              s"$pctUsableBasesOnTarget" -> right,
              s"$pctPfUniqueReadsAligned" -> right,
              badCycles.toString -> right,
              s"$pctChimeras" -> right,
              s"$pctTargetBases20" -> right,
              s"$pctTargetBases30" -> right,
              s"$pctTargetBases50" -> right,
              s"$coveragePerRead" -> right,
              s"$pctExcDupe" -> right,
              s"$pctExcMapQ" -> right,
              s"$pctExcBaseQ" -> right,
              s"$pctExcOverlap" -> right,
              s"$pctExcOffTarget" -> right,
              "wxs-perlane" -> right
            ))

      }
      .mkString("\n")

    val laneHeader = Csv.mkHeader(
      List("Proj", "Sample", "Analysis", "Run", "Lane", "CaptureKit"),
      List(
        "TOTAL_READS" -> right,
        "MEAN_TARGET_COVERAGE" -> right,
        "MeanTargetCoverageDupeIncl" -> right,
        "PF_READS" -> right,
        "PCT_PF_READS_ALIGNED" -> right,
        "PCT_USABLE_BASES_ON_TARGET" -> right,
        "PCT_PF_UQ_READS_ALIGNED" -> right,
        "BAD_CYCLES" -> right,
        "PCT_CHIMERAS" -> right,
        "PCT_TARGET_BASES_20X" -> right,
        "PCT_TARGET_BASES_30X" -> right,
        "PCT_TARGET_BASES_50X" -> right,
        "CoveragePerMillionRead" -> right,
        "PCT_EXC_DUPE" -> right,
        "PCT_EXC_MAPQ" -> right,
        "PCT_EXC_BASEQ" -> right,
        "PCT_EXC_OVERLAP" -> right,
        "PCT_EXC_OFF_TARGET" -> right,
        "rnaseq" -> right
      )
    )

    val sampleHeader = Csv.mkHeader(
      List("Proj", "Sample", "Analysis"),
      List(
        "GENOME_TERRITORY" -> right,
        "MEAN_COVERAGE" -> right,
        "PERCENT_DUPLICATION" -> right,
        "READ_PAIR_DUPLICATES" -> right,
        "READ_PAIR_OPTICAL_DUPLICATES" -> right,
        "GC" -> right,
        "MODE_INSERT_SIZE" -> right,
        "PCT_20X(Wgs)" -> right,
        "PCT_60X(Wgs)" -> right,
        "PCT_EXC_TOTAL" -> right,
        "TOTAL_SNPS(Capture)" -> right,
        "TOTAL_SNPS(Wgs)" -> right,
        "NUM_IN_DB_SNP" -> right,
        "NOVEL_SNPS" -> right,
        "DBSNP_TITV" -> right,
        "NOVEL_TITV" -> right,
        "TOTAL_INDELS(Capture)" -> right,
        "TOTAL_INDELS(Wgs)" -> right,
        "NUM_IN_DB_SNP_INDELS" -> right,
        "NOVEL_INDELS" -> right,
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
              analysisId -> left,
              s"${numberOfReads}" -> right,
              s"$meanReadLength" -> right,
              s"${uniquelyMappedReads}" -> right,
              s"${uniquelyMappedPercentage}" -> right,
              s"${multiplyMappedReads}" -> right,
              s"${multiplyMappedReadsPercentage}" -> right,
              "rna" -> right
            ))

      }
      .mkString("\n")

    val rnaHeader = Csv.mkHeader(
      List("Proj", "Sample", "AnalysisId"),
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
    val runTable = runHeader + "\n" + runLines

    laneTable + "\n" + sampleTable + "\n" + runTable + "\n" + rnaTable

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
         AnalysisId,
         List[(RunId, MeanCoverageResult)])],
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
              analysisId,
              _) =>
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

    val fastCoveragePerRun = sampleMetrics.flatMap { sampleMetric =>
      sampleMetric._8.map {
        case (runId, meanCoverage) =>
          (sampleMetric._1.project,
           sampleMetric._1.sampleId,
           runId,
           sampleMetric._7,
           meanCoverage)
      }
    }

    val runLines =
      fastCoveragePerRun
        .sortBy(_._2.toString)
        .sortBy(_._1.toString)
        .sortBy(_._3.toString)
        .map {
          case (sampleId, project, runId, analysisId, fastCoverage) =>
            Html.line(
              Seq(project -> left,
                  sampleId -> left,
                  analysisId -> left,
                  runId -> left,
                  f"${fastCoverage.all}%13.1fx" -> right,
                  f"${fastCoverage.target}%13.1fx" -> right,
              ))
        }
        .mkString("\n")

    val runHeader = Html.mkHeader(
      List("Proj", "Sample", "Analysis", "Run"),
      List(
        "FastWgsCoverage" -> right,
        "FastTargetCoverage" -> right
      )
    )

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
        "TOTAL_READS" -> right,
        "MEAN_TARGET_COVERAGE" -> right,
        "MeanTargetCoverageDupeIncl" -> right,
        "PF_READS" -> right,
        "PCT_PF_READS_ALIGNED" -> right,
        "PCT_USABLE_BASES_ON_TARGET" -> right,
        "PCT_PF_UQ_READS_ALIGNED(PFUniqueReadsAligned)" -> right,
        "BAD_CYCLES" -> right,
        "PCT_CHIMERAS" -> right,
        "PCT_TARGET_BASES_20X" -> right,
        "PCT_TARGET_BASES_30X" -> right,
        "PCT_TARGET_BASES_50X" -> right,
        "CoveragePerMillionRead" -> right,
        "PCT_EXC_DUPE" -> right,
        "PCT_EXC_MAPQ" -> right,
        "PCT_EXC_BASEQ" -> right,
        "PCT_EXC_OVERLAP" -> right,
        "PCT_EXC_OFF_TARGET" -> right,
      )
    )

    val sampleHeader = Html.mkHeader(
      List("Proj", "Sample", "Analysis"),
      List(
        "GENOME_TERRITORY(GenomeSize)" -> right,
        "MEAN_COVERAGE" -> right,
        "PERCENT_DUPLICATION" -> right,
        "READ_PAIR_DUPLICATES" -> right,
        "READ_PAIR_OPTICAL_DUPLICATES" -> right,
        "GC" -> right,
        "MODE_INSERT_SIZE" -> right,
        "PCT_20X(Wgs)" -> right,
        "PCT_60X(Wgs)" -> right,
        "PCT_EXC_TOTAL" -> right,
        "TOTAL_SNPS(Capture)" -> right,
        "TOTAL_SNPS(Wgs)" -> right,
        "NUM_IN_DB_SNP" -> right,
        "NOVEL_SNPS" -> right,
        "DBSNP_TITV" -> right,
        "NOVEL_TITV" -> right,
        "TOTAL_INDELS(Capture)" -> right,
        "TOTAL_INDELS(Wgs)" -> right,
        "NUM_IN_DB_SNP_INDELS" -> right,
        "NOVEL_INDELS" -> right
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
      List("Proj", "Sample", "AnalysisId"),
      List(
        "TotalReads" -> right,
        "MeanReadLength" -> right,
        "UniquelyMapped" -> right,
        "UniquelyMapped%" -> right,
        "Multimapped" -> right,
        "Multimapped%" -> right
      )
    )

    val rnaTable =
      if (rnaLines.isEmpty) ""
      else
        """<table style="border-collapse: collapse;">""" + rnaHeader + "\n<tbody>" + rnaLines + "</tbody></table>"
    val laneTable =
      if (laneLines.isEmpty) ""
      else
        """<table style="border-collapse: collapse;">""" + laneHeader + "\n<tbody>" + laneLines + "</tbody></table>"
    val sampleTable =
      if (sampleLines.isEmpty) ""
      else
        """<table style="border-collapse: collapse;">""" + sampleHeader + "\n<tbody>" + sampleLines + "</tbody></table>"
    val runTable =
      if (runLines.isEmpty) ""
      else
        """<table style="border-collapse: collapse;">""" + runHeader + "\n<tbody>" + runLines + "</tbody></table>"

    """<!DOCTYPE html><head></head><body>""" + laneTable + sampleTable + runTable + rnaTable + "</body>"

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
         AnalysisId,
         List[(RunId, MeanCoverageResult)])],
      rnaSeqMetrics: Seq[(AnalysisId, StarMetrics.Root)]): String = {

    val left = true
    val right = false

    case class AggregatedLaneMetrics(
        totalReads: Long,
        totalPfReads: Long,
        totalPfReadsAligned: Long,
        totalPfUniqueReads: Long,
        totalPfUniqueReadsAligned: Long,
        totalMeanTargetCoverage: Double,
        totalMeanTargetCoverageIncludingDuplicates: Double,
        totalCoveragePerRead: Double,
        totalPercentPfUniqueReadsAligned: Double,
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
          val totalPercentPfUniqueReadsAligned = totalPfUniqueReadsAligned.toDouble / totalPfUniqueReads
            .toDouble

          (projectAndSample,
           AggregatedLaneMetrics(
             totalReads = totalReads,
             totalPfReads = totalPfReads,
             totalPfReadsAligned = totalPfReadsAligned,
             totalPfUniqueReads = totalPfUniqueReads,
             totalPfUniqueReadsAligned = totalPfUniqueReadsAligned,
             totalMeanTargetCoverage = totalMeanTargetCoverage,
             totalMeanTargetCoverageIncludingDuplicates =
               totalMeanTargetCoverageIncludingDuplicates,
             totalCoveragePerRead = totalCoveragePerRead,
             totalPercentPfUniqueReadsAligned = totalPercentPfUniqueReadsAligned,
             totalPercentPfReadsAligned = totalPercentPfReadsAligned
           ))

      }

    val aggregatedLanesPerSamplePerRun = laneMetrics
      .groupBy(v => (v._1.project, v._1.sampleId, v._1.runId))
      .map {
        case (projectAndSampleAndRun, group) =>
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
          val totalPercentPfUniqueReadsAligned = totalPfUniqueReadsAligned.toDouble / totalPfUniqueReads
            .toDouble

          (projectAndSampleAndRun,
           AggregatedLaneMetrics(
             totalReads = totalReads,
             totalPfReads = totalPfReads,
             totalPfReadsAligned = totalPfReadsAligned,
             totalPfUniqueReads = totalPfUniqueReads,
             totalPfUniqueReadsAligned = totalPfUniqueReadsAligned,
             totalMeanTargetCoverage = totalMeanTargetCoverage,
             totalMeanTargetCoverageIncludingDuplicates =
               totalMeanTargetCoverageIncludingDuplicates,
             totalCoveragePerRead = totalCoveragePerRead,
             totalPercentPfUniqueReadsAligned = totalPercentPfUniqueReadsAligned,
             totalPercentPfReadsAligned = totalPercentPfReadsAligned
           ))

      }

    val fastCoveragePerRun = sampleMetrics.flatMap { sampleMetric =>
      sampleMetric._8.map {
        case (runId, meanCoverage) =>
          ((sampleMetric._1.project, sampleMetric._1.sampleId, runId),
           meanCoverage)
      }
    }.toMap

    val runLines = aggregatedLanesPerSamplePerRun.toSeq
      .sortBy(_._1._3.toString)
      .sortBy(_._1._2.toString)
      .sortBy(_._1._1.toString)
      .map {
        case ((project, sample, run), aggregatedLaneMetrics) =>
          val fastCoverage =
            fastCoveragePerRun.get((project, sample, run)).map(_.all)
          Html.line(
            Seq(
              project -> left,
              sample -> left,
              run -> left,
              f"${aggregatedLaneMetrics.totalMeanTargetCoverage}%13.1fx" -> right,
              f"${aggregatedLaneMetrics.totalMeanTargetCoverageIncludingDuplicates}%13.1fx" -> right,
              fastCoverage
                .map(all => f"${all}%13.1fx")
                .getOrElse("NA") -> right,
              f"${aggregatedLaneMetrics.totalReads / 1E6}%10.2fM" -> right,
              f"${aggregatedLaneMetrics.totalPfReads / 1E6}%10.2fM" -> right,
              f"${aggregatedLaneMetrics.totalPercentPfReadsAligned * 100}%6.2f%%" -> right,
              f"${aggregatedLaneMetrics.totalPfUniqueReads / 1E6}%10.2fM" -> right,
              f"${aggregatedLaneMetrics.totalPercentPfUniqueReadsAligned * 100}%6.2f%%" -> right
            ))

      }
      .mkString("\n")

    val runHeader = Html.mkHeader(
      List("Proj", "Sample", "Run"),
      List(
        "MEAN_TARGET_COVERAGE" -> right,
        "MeanTargetCoverageDupIncl" -> right,
        "FastWgsCoverage" -> right,
        "TOTAL_READS" -> right,
        "PF_READS" -> right,
        "PCT_PF_READS_ALIGNED" -> right,
        "PF_UNIQUE_READS" -> right,
        "PCT_PF_UQ_READS_ALIGNED" -> right,
      )
    )

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
              _,
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
              f"${wgsMetrics.metrics.meanCoverageIncludingDuplicates}%13.1fx" -> right,
              f"${aggregatedLaneMetrics.totalMeanTargetCoverage}%13.1fx" -> right,
              f"${aggregatedLaneMetrics.totalMeanTargetCoverageIncludingDuplicates}%13.1fx" -> right,
              f"${aggregatedLaneMetrics.totalReads / 1E6}%10.2fM" -> right,
              f"${aggregatedLaneMetrics.totalPfReads / 1E6}%10.2fM" -> right,
              f"${aggregatedLaneMetrics.totalPercentPfReadsAligned * 100}%6.2f%%" -> right,
              f"${aggregatedLaneMetrics.totalPfUniqueReads / 1E6}%10.2fM" -> right,
              f"${aggregatedLaneMetrics.totalPercentPfUniqueReadsAligned * 100}%6.2f%%" -> right,
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
        "TOTAL_READS" -> right,
        "MEAN_TARGET_COVERAGE" -> right,
        "MeanTargetCoverageDupeIncl" -> right,
        "PF_READS" -> right,
        "PCT_PF_READS_ALIGNED" -> right,
        "PCT_USABLE_BASES_ON_TARGET" -> right,
        "PCT_PF_UQ_READS_ALIGNED(PFUniqueReadsAligned)" -> right,
        "BAD_CYCLES" -> right,
        "PCT_CHIMERAS" -> right,
        "PCT_TARGET_BASES_20X" -> right,
        "PCT_TARGET_BASES_30X" -> right,
        "PCT_TARGET_BASES_50X" -> right,
        "CoveragePerMillionRead" -> right,
        "PCT_EXC_DUPE" -> right,
        "PCT_EXC_MAPQ" -> right,
        "PCT_EXC_BASEQ" -> right,
        "PCT_EXC_OVERLAP" -> right,
        "PCT_EXC_OFF_TARGET" -> right
      )
    )

    val sampleHeader = Html.mkHeader(
      List("Proj", "Sample"),
      List(
        "MEAN_COVERAGE(Wgs)" -> right,
        "MeanCoverageDupIncl(Wgs)" -> right,
        "MEAN_TARGET_COVERAGE" -> right,
        "MeanTargetCoverageDupIncl" -> right,
        "TOTAL_READS" -> right,
        "PF_READS" -> right,
        "PCT_PF_READS_ALIGNED" -> right,
        "PF_UNIQUE_READS" -> right,
        "PCT_PF_UQ_READS_ALIGNED" -> right,
        "PERCENT_DUPLICATION" -> right,
        "READ_PAIR_DUPLICATES" -> right,
        "READ_PAIR_OPTICAL_DUPLICATES" -> right,
        "MODE_INSERT_SIZE" -> right,
        "PCT_20X(Wgs)" -> right,
        "PCT_60X(Wgs)" -> right,
        "PCT_EXC_TOTAL" -> right,
        "TOTAL_SNPS(Capture)" -> right,
        "TOTAL_SNPS(Wgs)" -> right,
        "TOTAL_INDELS(Capture)" -> right,
        "TOTAL_INDELS(Wgs)" -> right
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
      List("Proj", "Sample"),
      List(
        "TotalReads" -> right,
        "MeanReadLength" -> right,
        "UniquelyMapped" -> right,
        "UniquelyMapped%" -> right,
        "Multimapped" -> right,
        "Multimapped%" -> right
      )
    )

    val rnaTable =
      if (rnaLines.isEmpty) ""
      else
        """<table style="border-collapse: collapse;">""" + rnaHeader + "\n<tbody>" + rnaLines + "</tbody></table>"
    val laneTable =
      if (laneLines.isEmpty) ""
      else
        """<table style="border-collapse: collapse;">""" + laneHeader + "\n<tbody>" + laneLines + "</tbody></table>"
    val sampleTable =
      if (sampleLines.isEmpty) ""
      else
        """<table style="border-collapse: collapse;">""" + sampleHeader + "\n<tbody>" + sampleLines + "</tbody></table>"
    val runTable =
      if (runLines.isEmpty) ""
      else
        """<table style="border-collapse: collapse;">""" + runHeader + "\n<tbody>" + runLines + "</tbody></table>"

    """<!DOCTYPE html><head></head><body>""" + runTable + sampleTable + laneTable + rnaTable + "</body>"

  }

  val runQCTable =
    AsyncTask[RunQCTableInput, RunQCTable]("__runqctable", 7) {
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
                                           m.analysisId,
                                           m.fastCoverages)

              (laneSpecificMetrics, sampleSpecificMetrics)

            }

          val parsedFiles =
            Future
              .traverse(sampleMetrics.toSeq)(parse)
              .map(pair => (pair.flatMap(_._1), pair.map(_._2)))

          val parsedRNAResults = Future.traverse(rnaAnalyses.toSeq) {
            case (analysisId,
                  StarResult(log, BamWithSampleMetadata(project, sample, _))) =>
              implicit val mat =
                computationEnvironment.components.actorMaterializer
              log.source
                .runFold(ByteString.empty)(_ ++ _)
                .map(_.utf8String)
                .map { content =>
                  (analysisId, StarMetrics.Root(content, project, sample))
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
