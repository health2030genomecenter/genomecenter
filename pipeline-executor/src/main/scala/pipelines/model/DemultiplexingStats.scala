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
            (this.ReadInfosForLanes ++ that.ReadInfosForLanes).distinct,
            (this.ConversionResults ++ that.ConversionResults)
              .groupBy(_.LaneNumber)
              .toSeq
              .map {
                case (_, group) => group.reduce(_ ++ _)
              },
            (this.UnknownBarcodes ++ that.UnknownBarcodes)
              .groupBy(_.Lane)
              .toSeq
              .map {
                case (_, group) => group.reduce(_ ++ _)
              }
          ))
  }

  object Root {
    implicit val encoder: Encoder[Root] =
      deriveEncoder[Root]
    implicit val decoder: Decoder[Root] =
      deriveDecoder[Root]

  }

  case class UnknownBarCodesPerLane(Lane: Int, Barcodes: Map[String, Long]) {
    def ++(that: UnknownBarCodesPerLane) = {
      require(Lane == that.Lane)
      UnknownBarCodesPerLane(
        Lane,
        tasks.util.addMaps(Barcodes, that.Barcodes)(_ + _)
      )
    }
  }

  case class ReadInfoPerLane(LaneNumber: Int, ReadInfos: Seq[ReadInfo])

  case class ReadInfo(Number: Int, NumCycles: Int, IsIndexedRead: Boolean)

  case class ConversionResultPerLane(LaneNumber: Int,
                                     TotalClustersRaw: Long,
                                     TotalClustersPF: Long,
                                     Yield: Long,
                                     DemuxResults: Seq[DemuxResultPerSample],
                                     Undetermined: UndeterminedResults) {
    def ++(that: ConversionResultPerLane) = {
      require(LaneNumber == that.LaneNumber)
      ConversionResultPerLane(
        LaneNumber = LaneNumber,
        TotalClustersRaw = TotalClustersRaw + that.TotalClustersRaw,
        TotalClustersPF = TotalClustersPF + that.TotalClustersPF,
        Yield = Yield + that.Yield,
        DemuxResults =
          (DemuxResults ++ that.DemuxResults).groupBy(_.SampleId).toSeq.map {
            case (_, group) => group.reduce(_ ++ _)
          },
        Undetermined = Undetermined ++ that.Undetermined
      )
    }
  }

  case class DemuxResultPerSample(
      SampleId: String,
      SampleName: String,
      IndexMetrics: Seq[IndexMetric],
      NumberReads: Long,
      Yield: Long,
      ReadMetrics: Seq[ReadMetric]
  ) {
    def ++(that: DemuxResultPerSample) = {
      require(SampleId == that.SampleId)
      DemuxResultPerSample(
        SampleId,
        SampleName,
        IndexMetrics = (IndexMetrics ++ that.IndexMetrics)
          .groupBy(_.IndexSequence)
          .toSeq
          .map {
            case (_, group) => group.reduce(_ ++ _)
          },
        NumberReads = NumberReads + that.NumberReads,
        Yield = Yield + that.Yield,
        ReadMetrics = merge(ReadMetrics, that.ReadMetrics)(_.ReadNumber)(_ ++ _)
      )

    }
  }

  def merge[T, K](a: Seq[T], b: Seq[T])(key: T => K)(op: (T, T) => T) =
    (a ++ b).groupBy(key).toSeq.map { case (_, group) => group.reduce(op) }

  case class UndeterminedResults(
      NumberReads: Long,
      Yield: Long,
      ReadMetrics: Seq[ReadMetric]
  ) {
    def ++(that: UndeterminedResults) = {
      UndeterminedResults(
        NumberReads + that.NumberReads,
        Yield + that.Yield,
        merge(ReadMetrics, that.ReadMetrics)(_.ReadNumber)(_ ++ _)
      )
    }
  }

  case class ReadMetric(
      ReadNumber: Int,
      Yield: Long,
      YieldQ30: Long,
      QualityScoreSum: Long,
      TrimmedBases: Long,
  ) {
    def ++(that: ReadMetric) = {
      require(ReadNumber == that.ReadNumber)
      ReadMetric(
        ReadNumber,
        Yield + that.Yield,
        YieldQ30 + that.YieldQ30,
        QualityScoreSum + that.QualityScoreSum,
        TrimmedBases + that.TrimmedBases
      )
    }
  }

  case class IndexMetric(
      IndexSequence: String,
      MismatchCounts: Map[String, Long]
  ) {
    def ++(that: IndexMetric) = {
      require(IndexSequence == that.IndexSequence)
      IndexMetric(
        IndexSequence,
        tasks.util.addMaps(MismatchCounts, that.MismatchCounts)(_ + _))
    }
  }

}

object DemultiplexingSummary {

  def fromStats(raw: DemultiplexingStats.Root,
                sampleIdToProject: Map[SampleId, Project],
                globalIndexSet: Set[String]): Root = {
    val runId = RunId(raw.RunId)
    val unknownBarCodesByLane =
      raw.UnknownBarcodes.map(ukb => ukb.Lane -> ukb).toMap

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

    val laneSummaries = raw.ConversionResults.map { conversionResultOfLane =>
      val lane = Lane(conversionResultOfLane.LaneNumber)
      val pctPf = 100d * conversionResultOfLane.TotalClustersPF.toDouble / conversionResultOfLane.TotalClustersRaw
      val top20UnknownBarcodes = unknownBarCodesByLane
        .get(conversionResultOfLane.LaneNumber)
        .map { unknownBarcodes =>
          unknownBarcodes.Barcodes.toSeq.sortBy(_._2).reverse.take(20)
        }
        .getOrElse(Nil)
        .map {
          case (idx, count) =>
            (idx,
             count,
             count / conversionResultOfLane.TotalClustersPF.toDouble)
        }

      val indexSwaps = {
        val frequentUnknownBarcodes = top20UnknownBarcodes.filter(_._3 >= 0.001)
        val indicesInThisSampleSheet: Set[String] =
          sampleSummaries.flatMap(_.indexSequence.split("\\+").toList).toSet
        val candidateIndexSwaps = frequentUnknownBarcodes.filter {
          case (idx: String, _, _) =>
            val splitted = idx.split("\\+")
            val bothAreIndicesGlobally = splitted.forall(globalIndexSet)
            val bothAreIndicesInThisSampleSheet =
              splitted.forall(indicesInThisSampleSheet)
            bothAreIndicesInThisSampleSheet || bothAreIndicesGlobally
        }
        candidateIndexSwaps.map {
          case (idx, count, fraction) =>
            IndexSwap(
              indexSequence = idx,
              count = count,
              fractionOfLane = fraction,
              presentInOtherLanes = sampleSummaries
                .filter(_.indexSequence == idx)
                .map(_.lane)
                .distinct
            )
        }

      }
      DemultiplexingLaneSummary(
        lane = lane,
        totalClustersRaw = conversionResultOfLane.TotalClustersRaw,
        totalClustersPF = conversionResultOfLane.TotalClustersPF,
        pctPFClusters = pctPf,
        topUnknownBarcodes = top20UnknownBarcodes,
        indexSwaps = indexSwaps
      )
    }

    Root(runId, sampleSummaries, laneSummaries)

  }

  case class SampleIndex(
      runId: RunId,
      project: Project,
      sampleId: SampleId,
      lane: Lane,
      indexOfSample: String,
      unexpectedIndicesSpottedInLane: Seq[(String, Double)]
  )
  object SampleIndex {
    implicit val encoder: Encoder[SampleIndex] =
      deriveEncoder[SampleIndex]
    implicit val decoder: Decoder[SampleIndex] =
      deriveDecoder[SampleIndex]
  }

  case class Root(
      runId: RunId,
      sampleSummaries: Seq[DemultiplexingSampleSummary],
      laneSummaries: Seq[DemultiplexingLaneSummary]
  ) {
    def sampleIndices = {
      val lanes =
        laneSummaries.map(laneSummary => laneSummary.lane -> laneSummary).toMap
      sampleSummaries
        .map { sampleSummary =>
          val lane = lanes(sampleSummary.lane)
          val contaminatingIndices = lane.indexSwaps.map(indexSwap =>
            indexSwap.indexSequence -> indexSwap.fractionOfLane)
          SampleIndex(sampleSummary.runId,
                      sampleSummary.project,
                      sampleSummary.sampleId,
                      sampleSummary.lane,
                      sampleSummary.indexSequence,
                      contaminatingIndices)
        }
    }
  }

  case class DemultiplexingLaneSummary(
      lane: Lane,
      totalClustersRaw: Long,
      totalClustersPF: Long,
      pctPFClusters: Double,
      topUnknownBarcodes: Seq[(String, Long, Double)],
      indexSwaps: Seq[IndexSwap]
  )

  case class IndexSwap(
      indexSequence: String,
      count: Long,
      fractionOfLane: Double,
      presentInOtherLanes: Seq[Lane]
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

    val indexSwapHeader =
      "!!!Unexpected indices:\nLane   Barcode            Count            %          OtherLanes"
    val indexSwapLines: Seq[String] = root.laneSummaries
      .map { l =>
        l.indexSwaps
          .map {
            case IndexSwap(barcode, count, fraction, otherLanes) =>
              f"${l.lane}%-7s$barcode%-12s$count%12s${fraction * 100}%12.2f%%${otherLanes.mkString(",")}%20s"
          }
          .mkString("\n")
      }
      .filterNot(_.isEmpty)

    val indexSwapSection =
      if (indexSwapLines.nonEmpty) {
        indexSwapHeader + "\n" + indexSwapLines.mkString("\n") + "\n\n"
      } else
        "Unexpected indices:\nNO UNEXPECTED KNOWN INDICES IN QUANTITY ABOVE 0.1%\n"

    val barcodeHeader =
      "Unknown barcodes:\nLane   Barcode            Count           %"
    val barcodesLines = root.laneSummaries.map { l =>
      l.topUnknownBarcodes
        .map {
          case (barcode, count, fraction) =>
            f"${l.lane}%-7s$barcode%-12s$count%12s${fraction * 100}%12.2f%%"
        }
        .mkString("\n")
    }

    val sampleHeader =
      "Samples:\nPrj                 SmplId              Lane   BCode             BCMismatch%     TotRds   Rd1_YieldQ30   Rd2_YieldQ30   Rd1_%Q30   Rd2_%Q30"

    val sampleLines = root.sampleSummaries.map { s =>
      import s._
      f"$project%-20s$sampleId%-20s$lane%-7s$indexSequence%-18s$indexMismatchRate%10.2f%%${totalReads / 1E6}%10.4fM${read1YieldQ30 / 1E6}%13.2fMb${read2YieldQ30 / 1E6}%13.2fMb$read1PctQ30%10.2f%%$read2PctQ30%10.2f%%"
    }

    s"RunId: ${root.runId}\n\n" + laneSummaryHeader + "\n" + laneSummaryLines
      .mkString("\n") + "\n\n" + sampleHeader + "\n" + sampleLines.mkString(
      "\n") + "\n\n" + indexSwapSection + "\n" + barcodeHeader + "\n" + barcodesLines
      .mkString("\n")

  }

}
