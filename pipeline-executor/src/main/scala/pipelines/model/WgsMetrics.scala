package org.gc.pipelines.model

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.generic.auto._

object WgsMetrics {

  case class Root(
      project: Project,
      sampleId: SampleId,
      metrics: Metrics
  )

  case class Metrics(
      genomeTerritory: Long,
      meanCoverage: Double,
      sdCoverage: Double,
      medianCoverage: Double,
      madCoverage: Double,
      pctExcludedMapQ: Double,
      pctExcludedDuplication: Double,
      pctExcludedUnpaired: Double,
      pctExcludedBaseQ: Double,
      pctExcludedOverlap: Double,
      pctExcludedCapped: Double,
      pctExcludedTotal: Double,
      pctCoverage1x: Double,
      pctCoverage10x: Double,
      pctCoverage20x: Double,
      pctCoverage60x: Double,
      pctCoverage100x: Double
  ) {
    def meanCoverageIncludingDuplicates: Double =
      meanCoverage / (1d - pctExcludedDuplication)
  }

  object Root {

    def apply(picardFileContents: String,
              project: Project,
              sampleId: SampleId): Root = {

      val lines = scala.io.Source
        .fromString(picardFileContents)
        .getLines
        .filterNot(line => line.isEmpty)
        .dropWhile(line => !line.startsWith("GENOME_TERRITORY"))
        .takeWhile(line => !line.startsWith("##"))
        .map(_.split("\\t").toVector)
        .toVector
      val header = lines.head
      def parseHeader(headerFields: Seq[String]) = {
        val idx = headerFields.map { h =>
          h -> header.indexOf(h)
        }.toMap

        (field: String, line: Vector[String]) =>
          if (idx(field) >= line.length) "" else line(idx(field))

      }

      object H {
        val GENOME_TERRITORY = "GENOME_TERRITORY"
        val MEAN_COVERAGE = "MEAN_COVERAGE"
        val SD_COVERAGE = "SD_COVERAGE"
        val MEDIAN_COVERAGE = "MEDIAN_COVERAGE"
        val MAD_COVERAGE = "MAD_COVERAGE"
        val PCT_EXC_MAPQ = "PCT_EXC_MAPQ"
        val PCT_EXC_DUPE = "PCT_EXC_DUPE"
        val PCT_EXC_UNPAIRED = "PCT_EXC_UNPAIRED"
        val PCT_EXC_BASEQ = "PCT_EXC_BASEQ"
        val PCT_EXC_OVERLAP = "PCT_EXC_OVERLAP"
        val PCT_EXC_CAPPED = "PCT_EXC_CAPPED"
        val PCT_EXC_TOTAL = "PCT_EXC_TOTAL"
        val PCT_1X = "PCT_1X"
        val PCT_10X = "PCT_10X"
        val PCT_20X = "PCT_20X"
        val PCT_60X = "PCT_60X"
        val PCT_100X = "PCT_100X"

        val allHeaders = List(
          GENOME_TERRITORY,
          MEAN_COVERAGE,
          SD_COVERAGE,
          MEDIAN_COVERAGE,
          MAD_COVERAGE,
          PCT_EXC_MAPQ,
          PCT_EXC_DUPE,
          PCT_EXC_UNPAIRED,
          PCT_EXC_BASEQ,
          PCT_EXC_OVERLAP,
          PCT_EXC_CAPPED,
          PCT_EXC_TOTAL,
          PCT_1X,
          PCT_10X,
          PCT_20X,
          PCT_60X,
          PCT_100X
        )
      }

      val get = parseHeader(H.allHeaders)

      val dataLines = lines
        .drop(1)

      dataLines.map { line =>
        val g = get(_: String, line)

        val metrics = Metrics(
          genomeTerritory = g(H.GENOME_TERRITORY).toLong,
          meanCoverage = g(H.MEAN_COVERAGE).toDouble,
          sdCoverage = g(H.SD_COVERAGE).toDouble,
          medianCoverage = g(H.MEDIAN_COVERAGE).toDouble,
          madCoverage = g(H.MAD_COVERAGE).toDouble,
          pctExcludedMapQ = g(H.PCT_EXC_MAPQ).toDouble,
          pctExcludedDuplication = g(H.PCT_EXC_DUPE).toDouble,
          pctExcludedUnpaired = g(H.PCT_EXC_UNPAIRED).toDouble,
          pctExcludedBaseQ = g(H.PCT_EXC_BASEQ).toDouble,
          pctExcludedOverlap = g(H.PCT_EXC_OVERLAP).toDouble,
          pctExcludedCapped = g(H.PCT_EXC_CAPPED).toDouble,
          pctExcludedTotal = g(H.PCT_EXC_TOTAL).toDouble,
          pctCoverage1x = g(H.PCT_1X).toDouble,
          pctCoverage10x = g(H.PCT_10X).toDouble,
          pctCoverage20x = g(H.PCT_20X).toDouble,
          pctCoverage60x = g(H.PCT_60X).toDouble,
          pctCoverage100x = g(H.PCT_100X).toDouble
        )

        Root(metrics = metrics, sampleId = sampleId, project = project)

      }.head

    }

    implicit val encoder: Encoder[Root] =
      deriveEncoder[Root]
    implicit val decoder: Decoder[Root] =
      deriveDecoder[Root]

  }

}
