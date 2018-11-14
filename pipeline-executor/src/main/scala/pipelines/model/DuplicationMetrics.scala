package org.gc.pipelines.model

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.generic.auto._

object DuplicationMetrics {

  case class Root(
      project: Project,
      sampleId: SampleId,
      metrics: Metrics
  )

  case class Metrics(
      readPairDuplicates: Long,
      readPairOpticalDuplicates: Long,
      pctDuplication: Double
  )

  object Root {

    def apply(picardFileContents: String,
              project: Project,
              sampleId: SampleId): Root = {

      val lines = scala.io.Source
        .fromString(picardFileContents)
        .getLines
        .filterNot(line => line.isEmpty)
        .dropWhile(line => !line.startsWith("LIBRARY"))
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
        val LIBRARY = "LIBRARY"
        val READ_PAIR_DUPLICATES = "READ_PAIR_DUPLICATES"
        val READ_PAIR_OPTICAL_DUPLICATES = "READ_PAIR_OPTICAL_DUPLICATES"
        val PERCENT_DUPLICATION = "PERCENT_DUPLICATION"

        val allHeaders = List(
          LIBRARY,
          READ_PAIR_DUPLICATES,
          READ_PAIR_OPTICAL_DUPLICATES,
          PERCENT_DUPLICATION
        )
      }

      val get = parseHeader(H.allHeaders)

      val dataLines = lines
        .drop(1)

      dataLines.map { line =>
        val g = get(_: String, line)
        val metrics = Metrics(
          readPairDuplicates = g(H.READ_PAIR_DUPLICATES).toLong,
          readPairOpticalDuplicates = g(H.READ_PAIR_OPTICAL_DUPLICATES).toLong,
          pctDuplication = g(H.PERCENT_DUPLICATION).toDouble
        )

        val sampleIdInFile = g(H.LIBRARY)

        require(
          project + "." + sampleId == sampleIdInFile,
          s"unexpected project and sampleId. picard output file has $sampleIdInFile we have prj:$project and sample: $sampleId"
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
