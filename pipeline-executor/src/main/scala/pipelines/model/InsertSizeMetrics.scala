package org.gc.pipelines.model

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.generic.auto._

object InsertSizeMetrics {

  case class Root(
      project: Project,
      sampleId: SampleId,
      metrics: Metrics
  )

  case class Metrics(
      medianInsertSize: Double,
      madInsertSize: Double,
      modeInsertSize: Int,
      meanInsertSize: Double,
      sdInsertSize: Double
  )

  object Root {

    def apply(picardFileContents: String,
              project: Project,
              sampleId: SampleId): Root = {

      val lines = scala.io.Source
        .fromString(picardFileContents)
        .getLines
        .filterNot(line => line.isEmpty)
        .dropWhile(line => !line.startsWith("MEDIAN_INSERT_SIZE"))
        .takeWhile(line => !line.startsWith("#"))
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
        val MEDIAN_INSERT_SIZE = "MEDIAN_INSERT_SIZE"
        val MEDIAN_ABSOLUTE_DEVIATION = "MEDIAN_ABSOLUTE_DEVIATION"
        val MODE_INSERT_SIZE = "MODE_INSERT_SIZE"
        val MEAN_INSERT_SIZE = "MEAN_INSERT_SIZE"
        val STANDARD_DEVIATION = "STANDARD_DEVIATION"

        val allHeaders = List(MEDIAN_INSERT_SIZE,
                              MEDIAN_ABSOLUTE_DEVIATION,
                              MODE_INSERT_SIZE,
                              MEAN_INSERT_SIZE,
                              STANDARD_DEVIATION)
      }

      val get = parseHeader(H.allHeaders)

      val dataLines = lines
        .drop(1)

      dataLines.map { line =>
        val g = get(_: String, line)
        val metrics = Metrics(
          medianInsertSize = g(H.MEDIAN_INSERT_SIZE).toDouble,
          madInsertSize = g(H.MEDIAN_ABSOLUTE_DEVIATION).toDouble,
          modeInsertSize = g(H.MODE_INSERT_SIZE).toInt,
          meanInsertSize = g(H.MEAN_INSERT_SIZE).toDouble,
          sdInsertSize = g(H.STANDARD_DEVIATION).toDouble
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
