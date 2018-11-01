package org.gc.pipelines.model

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.generic.auto._

object StarMetrics {

  case class Root(
      project: Project,
      sampleId: SampleId,
      runId: RunId,
      lane: Lane,
      metrics: Metrics
  )

  case class Metrics(
      numberOfReads: Long,
      meanReadLength: Double,
      uniquelyMappedReads: Long,
      uniquelyMappedPercentage: Double,
      multiplyMappedReads: Long,
      multiplyMappedReadsPercentage: Double
  )

  object Root {
    def apply(starFinalLogContents: String,
              project: Project,
              sampleId: SampleId,
              runId: RunId,
              lane: Lane): Root = {
      val lines = scala.io.Source
        .fromString(starFinalLogContents)
        .getLines
        .map { line =>
          line.split("\\|").toList
        }
        .toList
      def extract[T](predicate: String)(t: String => T) =
        lines
          .find { h =>
            h.head.contains(predicate)
          }
          .map {
            case List(_, v) => t(v.trim)
          }

      Root(
        project,
        sampleId,
        runId,
        lane,
        Metrics(
          numberOfReads = extract("Number of input reads")(_.toLong).get,
          meanReadLength = extract("Average input read length")(_.toDouble).get,
          uniquelyMappedReads =
            extract("Uniquely mapped reads number")(_.toLong).get,
          uniquelyMappedPercentage = extract("Uniquely mapped reads %")(
            _.dropRight(1).toDouble / 100d).get,
          multiplyMappedReads =
            extract("Number of reads mapped to multiple loci")(_.toLong).get,
          multiplyMappedReadsPercentage =
            extract("% of reads mapped to multiple loci")(
              _.dropRight(1).toDouble / 100d).get
        )
      )
    }
    implicit val encoder: Encoder[Root] =
      deriveEncoder[Root]
    implicit val decoder: Decoder[Root] =
      deriveDecoder[Root]

  }
}
