package org.gc.pipelines.model

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.generic.auto._

object DemultiplexingStats {
  case class Root(
      Flowcell: String,
      RunNumber: Int,
      RunId: String,
      ReadInfosForLanes: Seq[ReadInfoPerLane],
      ConversionResults: Seq[ConversionResultPerLane],
      UnknownBarcodes: Seq[UnknownBarCodesPerLane]
  ) {
    def ++(that: Root): Either[String, Root] =
      if (this.Flowcell != that.Flowcell || this.RunNumber != that.RunNumber || this.RunId != that.RunId)
        Left("flowcell and run must match")
      else
        Right(
          Root(
            Flowcell = Flowcell,
            RunNumber = RunNumber,
            RunId = RunId,
            this.ReadInfosForLanes ++ that.ReadInfosForLanes,
            this.ConversionResults ++ that.ConversionResults,
            this.UnknownBarcodes ++ that.UnknownBarcodes
          ))
  }

  object Root {
    implicit val encoder: Encoder[Root] =
      deriveEncoder[Root]
    implicit val decoder: Decoder[Root] =
      deriveDecoder[Root]

  }

  case class UnknownBarCodesPerLane(Lane: Int, Barcodes: Map[String, Long])

  case class ReadInfoPerLane(LaneNumber: Int, ReadInfos: Seq[ReadInfo])

  case class ReadInfo(Number: Int, NumCycles: Int, IsIndexedRead: Boolean)

  case class ConversionResultPerLane(LaneNumber: Int,
                                     TotalClustersRaw: Long,
                                     TotalClustersPF: Long,
                                     Yield: Long,
                                     DemuxResults: Seq[DemuxResultPerSample],
                                     Undetermined: UndeterminedResuls)

  case class DemuxResultPerSample(
      SampleId: String,
      SampleName: String,
      IndexMetrics: Seq[IndexMetric],
      NumberReads: Long,
      Yield: Long,
      ReadMetrics: Seq[ReadMetric]
  )

  case class UndeterminedResuls(
      NumberReads: Long,
      Yield: Long,
      ReadMetrics: Seq[ReadMetric]
  )

  case class ReadMetric(
      ReadNumber: Int,
      Yield: Long,
      YieldQ30: Long,
      QualityScoreSum: Long,
      TrimmedBases: Long,
  )

  case class IndexMetric(
      IndexSequence: String,
      MismatchCounts: Map[String, Long]
  )

}

object DemultiplexingSummary {

  def fromStats(raw: DemultiplexingStats.Root,
                sampleIdToProject: Map[SampleId, Project]): Root = {
    val runId = RunId(raw.RunId)
    val unknownBarCodesByLane =
      raw.UnknownBarcodes.map(ukb => ukb.Lane -> ukb).toMap

    val laneSummaries = raw.ConversionResults.map { conversionResultOfLane =>
      val lane = Lane(conversionResultOfLane.LaneNumber)
      val pctPf = 100d * conversionResultOfLane.TotalClustersPF.toDouble / conversionResultOfLane.TotalClustersRaw
      val top20UnknownBarcodes = unknownBarCodesByLane
        .get(conversionResultOfLane.LaneNumber)
        .map { unknownBarcodes =>
          unknownBarcodes.Barcodes.toSeq.sortBy(_._2).reverse.take(20)
        }
        .getOrElse(Nil)
      DemultiplexingLaneSummary(
        lane = lane,
        totalClustersRaw = conversionResultOfLane.TotalClustersRaw,
        totalClustersPF = conversionResultOfLane.TotalClustersPF,
        pctPFClusters = pctPf,
        topUnknownBarcodes = top20UnknownBarcodes
      )
    }

    val sampleSummaries = raw.ConversionResults.flatMap {
      conversionResultOfLane =>
        val lane = Lane(conversionResultOfLane.LaneNumber)
        val demuxed = conversionResultOfLane.DemuxResults
        val undetermined = DemultiplexingStats.DemuxResultPerSample(
          SampleId = "Undetermined",
          SampleName = "Undetermined",
          IndexMetrics = Nil,
          NumberReads = conversionResultOfLane.Undetermined.NumberReads,
          Yield = conversionResultOfLane.Undetermined.Yield,
          ReadMetrics = conversionResultOfLane.Undetermined.ReadMetrics
        )

        (demuxed :+ undetermined).map { demuxResultOfSample =>
          val read1Metrics = demuxResultOfSample.ReadMetrics
            .find(_.ReadNumber == 1)
          val read2Metrics = demuxResultOfSample.ReadMetrics
            .find(_.ReadNumber == 2)

          val sampleId = SampleId(demuxResultOfSample.SampleId)
          DemultiplexingSampleSummary(
            project = sampleIdToProject.get(sampleId).getOrElse(Project("NA")),
            sampleId = sampleId,
            lane = lane,
            runId = runId,
            indexSequence = demuxResultOfSample.IndexMetrics.headOption
              .map(_.IndexSequence)
              .getOrElse("NA"),
            indexMismatchRate = demuxResultOfSample.IndexMetrics.headOption
              .map { indexMetric =>
                val inperfectmatches = indexMetric.MismatchCounts
                  .filter(_._1 != "0")
                  .values
                  .sum
                  .toDouble

                inperfectmatches / indexMetric.MismatchCounts.values.sum * 100
              }
              .getOrElse(-1d),
            totalReads = demuxResultOfSample.NumberReads,
            read1QualityScoreSum = read1Metrics
              .map(_.QualityScoreSum)
              .getOrElse(-1L),
            read2QualityScoreSum = read2Metrics
              .map(_.QualityScoreSum)
              .getOrElse(-1L),
            totalYield = demuxResultOfSample.Yield,
            read1Yield = read1Metrics
              .map(_.Yield)
              .getOrElse(-1L),
            read2Yield = read2Metrics
              .map(_.Yield)
              .getOrElse(-1L),
            read1YieldQ30 = read1Metrics
              .map(_.YieldQ30)
              .getOrElse(-1L),
            read2YieldQ30 = read2Metrics
              .map(_.YieldQ30)
              .getOrElse(-1L),
            read1PctQ30 = read1Metrics
              .map(d => d.YieldQ30.toDouble / (d.Yield) * 100)
              .getOrElse(-1d),
            read2PctQ30 = read2Metrics
              .map(d => d.YieldQ30.toDouble / (d.Yield) * 100)
              .getOrElse(-1d),
          )
        }
    }

    Root(runId, sampleSummaries, laneSummaries)

  }

  case class Root(
      runId: RunId,
      sampleSummaries: Seq[DemultiplexingSampleSummary],
      laneSummaries: Seq[DemultiplexingLaneSummary]
  )

  case class DemultiplexingLaneSummary(
      lane: Lane,
      totalClustersRaw: Long,
      totalClustersPF: Long,
      pctPFClusters: Double,
      topUnknownBarcodes: Seq[(String, Long)]
  )

  case class DemultiplexingSampleSummary(
      project: Project,
      sampleId: SampleId,
      lane: Lane,
      runId: RunId,
      indexSequence: String,
      indexMismatchRate: Double,
      totalReads: Long,
      read1QualityScoreSum: Long,
      read2QualityScoreSum: Long,
      totalYield: Long,
      read1Yield: Long,
      read2Yield: Long,
      read1YieldQ30: Long,
      read2YieldQ30: Long,
      read1PctQ30: Double,
      read2PctQ30: Double,
  )

  def renderAsTable(root: Root) = {
    val laneSummaryHeader =
      "Lanes:\nLane     TotClustRaw   TotClustPF       %PF"
    val laneSummaryLines = root.laneSummaries.map { l =>
      f"${l.lane}%-7s${l.totalClustersRaw / 1E6}%12.2fM${l.totalClustersPF / 1E6}%12.2fM${l.pctPFClusters}%10.2f"
    }
    val barcodeHeader =
      "Unknown barcodes:\nLane   Barcode            Count"
    val barcodesLines = root.laneSummaries.map { l =>
      l.topUnknownBarcodes
        .map {
          case (barcode, count) => f"${l.lane}%-7s$barcode%-12s$count%12s"
        }
        .mkString("\n")
    }

    val sampleHeader =
      "Samples:\nPrj           SmplId        Lane   BCode             BCMismatch%     TotRds    Rd1_YieldQ30   Rd2_YieldQ30   Rd1_%Q30   Rd2_%Q30"

    val sampleLines = root.sampleSummaries.map { s =>
      import s._
      f"$project%-14s$sampleId%-14s$lane%-7s$indexSequence%-18s$indexMismatchRate%10.2f%%${totalReads / 1E6}%10.4fM${read1YieldQ30 / 1E6}%13.2fMb${read2YieldQ30 / 1E6}%13.2fMb$read1PctQ30%10.2f%%$read2PctQ30%10.2f%%"
    }

    s"RunId: ${root.runId}\n\n" + laneSummaryHeader + "\n" + laneSummaryLines
      .mkString("\n") + "\n\n" + sampleHeader + "\n" + sampleLines.mkString(
      "\n") + "\n\n" + barcodeHeader + "\n" + barcodesLines
      .mkString("\n")

  }

}
