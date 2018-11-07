package org.gc.pipelines.model

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.generic.auto._

object VariantCallingMetrics {

  case class Root(
      project: Project,
      sampleId: SampleId,
      runId: RunId,
      metrics: Metrics
  )

  case class Metrics(
      totalSnps: Int,
      snpsInDbSnp: Int,
      novelSnp: Int,
      dbSnpTiTv: Double,
      novelTiTv: Double,
      totalIndel: Int,
      indelsInDbSnp: Int,
      novelIndel: Int
  )

  object Root {

    def apply(picardFileContents: String,
              project: Project,
              sampleId: SampleId,
              runId: RunId): Root = {

      val lines = scala.io.Source
        .fromString(picardFileContents)
        .getLines
        .filterNot(line => line.isEmpty)
        .dropWhile(line => !line.startsWith("TOTAL_SNPS"))
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
        val TOTAL_SNPS = "TOTAL_SNPS"
        val NUM_IN_DB_SNP = "NUM_IN_DB_SNP"
        val NOVEL_SNPS = "NOVEL_SNPS"
        val DBSNP_TITV = "DBSNP_TITV"
        val NOVEL_TITV = "NOVEL_TITV"
        val TOTAL_INDELS = "TOTAL_INDELS"
        val NOVEL_INDELS = "NOVEL_INDELS"
        val NUM_IN_DB_SNP_INDELS = "NUM_IN_DB_SNP_INDELS"

        val allHeaders = List(
          TOTAL_SNPS,
          NUM_IN_DB_SNP,
          NOVEL_SNPS,
          DBSNP_TITV,
          NOVEL_TITV,
          TOTAL_INDELS,
          NOVEL_INDELS,
          NUM_IN_DB_SNP_INDELS
        )
      }

      val get = parseHeader(H.allHeaders)

      val dataLines = lines
        .drop(1)

      dataLines.map { line =>
        val g = get(_: String, line)

        val metrics = Metrics(
          totalSnps = g(H.TOTAL_SNPS).toInt,
          snpsInDbSnp = g(H.NUM_IN_DB_SNP).toInt,
          novelSnp = g(H.NOVEL_SNPS).toInt,
          dbSnpTiTv = g(H.DBSNP_TITV).toDouble,
          novelTiTv = g(H.NOVEL_TITV).toDouble,
          totalIndel = g(H.TOTAL_INDELS).toInt,
          indelsInDbSnp = g(H.NOVEL_INDELS).toInt,
          novelIndel = g(H.NUM_IN_DB_SNP_INDELS).toInt
        )

        Root(metrics = metrics,
             sampleId = sampleId,
             project = project,
             runId = runId)

      }.head

    }

    implicit val encoder: Encoder[Root] =
      deriveEncoder[Root]
    implicit val decoder: Decoder[Root] =
      deriveDecoder[Root]

  }

}
