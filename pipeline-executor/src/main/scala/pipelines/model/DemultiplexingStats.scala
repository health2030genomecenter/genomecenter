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
                                     DemuxResults: Seq[DemuxResultPerSample])

  case class DemuxResultPerSample(
      SampleId: String,
      SampleName: String,
      IndexMetrics: Seq[IndexMetric],
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
