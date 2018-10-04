package org.gc.pipelines.model

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.generic.auto._

object AlignmentSummaryMetrics {

  case class Root(
      project: Project,
      sampleId: SampleId,
      runId: RunId,
      lane: Lane,
      pairMetrics: Metrics
  )

  case class Metrics(
      totalReads: Long,
      pfReads: Long,
      pctPfReads: Double,
      pfReadsAligned: Long,
      pctPfReadsAligned: Double,
      pfHqReadsAlinged: Long,
      readsAlignedInPairs: Long,
      pctReadsAlignedInPairs: Double,
      badCycles: Int,
      strandBalance: Double,
      pctChimeras: Double,
      pctAdapter: Double
  )

  object Root {

    def apply(picardFileContents: String,
              project: Project,
              sampleId: SampleId,
              runId: RunId): Seq[Root] = {

      val lines = scala.io.Source
        .fromString(picardFileContents)
        .getLines
        .filterNot(line => line.isEmpty || line.startsWith("#"))
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

      def laneFromReadGroup(readGroup: String): Lane =
        Lane(readGroup.split("\\.").last)

      object H {
        val rg = "READ_GROUP"
        val sample = "SAMPLE"
        val category = "CATEGORY"
        val totalReads = "TOTAL_READS"
        val pfReads = "PF_READS"
        val pctPfReads = "PCT_PF_READS"
        val pfReadsAligned = "PF_READS_ALIGNED"
        val pctPfReadsAligned = "PCT_PF_READS_ALIGNED"
        val pfHqReadsAlinged = "PF_HQ_ALIGNED_READS"
        val readsAlignedInPairs = "READS_ALIGNED_IN_PAIRS"
        val pctReadsAlignedInPairs = "PCT_READS_ALIGNED_IN_PAIRS"
        val badCycles = "BAD_CYCLES"
        val strandBalance = "STRAND_BALANCE"
        val pctChimeras = "PCT_CHIMERAS"
        val pctAdapter = "PCT_ADAPTER"
        val allHeaders = List(
          category,
          rg,
          sample,
          totalReads,
          pfReads,
          pctPfReads,
          pfReadsAligned,
          pctPfReadsAligned,
          pfHqReadsAlinged,
          readsAlignedInPairs,
          pctReadsAlignedInPairs,
          badCycles,
          strandBalance,
          pctChimeras,
          pctAdapter
        )
      }

      val get = parseHeader(H.allHeaders)

      val readGroupPairLines = lines
        .drop(1)
        .filter(l => get(H.rg, l).nonEmpty)
        .filter(l => get(H.category, l) == "PAIR")

      readGroupPairLines.map { line =>
        val g = get(_: String, line)

        val metrics = Metrics(
          totalReads = g(H.totalReads).toLong,
          pfReads = g(H.pfReads).toLong,
          pctPfReads = g(H.pctPfReads).toDouble,
          pfReadsAligned = g(H.pfReadsAligned).toLong,
          pctPfReadsAligned = g(H.pctPfReadsAligned).toDouble,
          pfHqReadsAlinged = g(H.pfHqReadsAlinged).toLong,
          readsAlignedInPairs = g(H.readsAlignedInPairs).toLong,
          pctReadsAlignedInPairs = g(H.pctReadsAlignedInPairs).toDouble,
          badCycles = g(H.badCycles).toInt,
          strandBalance = g(H.strandBalance).toDouble,
          pctChimeras = g(H.pctChimeras).toDouble,
          pctAdapter = g(H.pctAdapter).toDouble
        )

        val lane = laneFromReadGroup(g(H.rg))
        val sampleIdInFile = g(H.sample)
        val readGroupInFile = g(H.rg)

        require(
          runId + "." + lane == readGroupInFile,
          s"unexpected run and lane. picard output file has $readGroupInFile we have run:$runId and lane: $lane")
        require(
          project + "." + sampleId == sampleIdInFile,
          s"unexpected project and sampleId. picard output file has $sampleIdInFile we have prj:$project and sample: $sampleId"
        )

        Root(pairMetrics = metrics,
             sampleId = sampleId,
             project = project,
             runId = runId,
             lane = lane)

      }

    }

    implicit val encoder: Encoder[Root] =
      deriveEncoder[Root]
    implicit val decoder: Decoder[Root] =
      deriveDecoder[Root]

  }

}
