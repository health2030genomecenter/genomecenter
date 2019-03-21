package org.gc.pipelines.model

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.generic.auto._

object HsMetrics {

  case class Root(
      project: Project,
      sampleId: SampleId,
      runId: RunId,
      lane: Lane,
      metrics: Metrics
  )

  case class Metrics(
      baitSet: String,
      genomeSize: Long,
      targetTerritory: Long,
      totalReads: Long,
      pfReads: Long,
      pfUniqueReads: Long,
      pfUniqueReadsAligned: Long,
      pctPfUniqueReadsAligned: Double,
      meanTargetCoverage: Double,
      medianTargetCoverage: Double,
      maxTargetCoverage: Double,
      pctTargetBases10: Double,
      pctTargetBases20: Double,
      pctTargetBases30: Double,
      pctTargetBases40: Double,
      pctTargetBases50: Double,
      pctUsableBasesOnTarget: Double,
      pctExcDupe: Double,
      pctExcMapQ: Double,
      pctExcBaseQ: Double,
      pctExcOverlap: Double,
      pctExcOffTarget: Double
  ) {
    def meanTargetCoverageIncludingDuplicates: Double =
      meanTargetCoverage / (1d - pctExcDupe)
  }

  object Root {

    def apply(picardFileContents: String,
              project: Project,
              sampleId: SampleId): Seq[Root] = {

      val lines = scala.io.Source
        .fromString(picardFileContents)
        .getLines
        .filterNot(line => line.isEmpty)
        .dropWhile(line => !line.startsWith("BAIT_SET"))
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

      def laneFromReadGroup(readGroup: String): Lane =
        Lane(readGroup.split("\\.").last.toInt)

      def runIdFromReadGroup(readGroup: String): RunId =
        RunId(readGroup.split("\\.").head.trim)

      object H {
        val READ_GROUP = "READ_GROUP"
        val SAMPLE = "SAMPLE"
        val BAIT_SET = "BAIT_SET"
        val TOTAL_READS = "TOTAL_READS"
        val PF_READS = "PF_READS"
        val GENOME_SIZE = "GENOME_SIZE"
        val TARGET_TERRITORY = "TARGET_TERRITORY"
        val PF_UNIQUE_READS = "PF_UNIQUE_READS"
        val PF_UQ_READS_ALIGNED = "PF_UQ_READS_ALIGNED"
        val PCT_PF_UQ_READS_ALIGNED = "PCT_PF_UQ_READS_ALIGNED"
        val MEAN_TARGET_COVERAGE = "MEAN_TARGET_COVERAGE"
        val MEDIAN_TARGET_COVERAGE = "MEDIAN_TARGET_COVERAGE"
        val MAX_TARGET_COVERAGE = "MAX_TARGET_COVERAGE"
        val PCT_TARGET_BASES_10X = "PCT_TARGET_BASES_10X"
        val PCT_TARGET_BASES_20X = "PCT_TARGET_BASES_20X"
        val PCT_TARGET_BASES_30X = "PCT_TARGET_BASES_30X"
        val PCT_TARGET_BASES_40X = "PCT_TARGET_BASES_40X"
        val PCT_TARGET_BASES_50X = "PCT_TARGET_BASES_50X"
        val PCT_USABLE_BASES_ON_TARGET = "PCT_USABLE_BASES_ON_TARGET"
        val PCT_EXC_DUPE = "PCT_EXC_DUPE"
        val PCT_EXC_MAPQ = "PCT_EXC_MAPQ"
        val PCT_EXC_BASEQ = "PCT_EXC_BASEQ"
        val PCT_EXC_OVERLAP = "PCT_EXC_OVERLAP"
        val PCT_EXC_OFF_TARGET = "PCT_EXC_OFF_TARGET"

        val allHeaders = List(
          READ_GROUP,
          SAMPLE,
          BAIT_SET,
          TOTAL_READS,
          PF_READS,
          GENOME_SIZE,
          TARGET_TERRITORY,
          PF_UNIQUE_READS,
          PF_UQ_READS_ALIGNED,
          PCT_PF_UQ_READS_ALIGNED,
          MEAN_TARGET_COVERAGE,
          MEDIAN_TARGET_COVERAGE,
          MAX_TARGET_COVERAGE,
          PCT_TARGET_BASES_10X,
          PCT_TARGET_BASES_20X,
          PCT_TARGET_BASES_30X,
          PCT_TARGET_BASES_40X,
          PCT_TARGET_BASES_50X,
          PCT_USABLE_BASES_ON_TARGET,
          PCT_EXC_DUPE,
          PCT_EXC_MAPQ,
          PCT_EXC_BASEQ,
          PCT_EXC_OVERLAP,
          PCT_EXC_OFF_TARGET
        )
      }

      val get = parseHeader(H.allHeaders)

      val readGroupLines = lines
        .drop(1)
        .filter(l => get(H.READ_GROUP, l).nonEmpty)

      readGroupLines.map { line =>
        val g = get(_: String, line)

        val metrics = Metrics(
          baitSet = g(H.BAIT_SET),
          genomeSize = g(H.GENOME_SIZE).toLong,
          targetTerritory = g(H.TARGET_TERRITORY).toLong,
          totalReads = g(H.TOTAL_READS).toLong,
          pfReads = g(H.PF_READS).toLong,
          pfUniqueReads = g(H.PF_UNIQUE_READS).toLong,
          pfUniqueReadsAligned = g(H.PF_UQ_READS_ALIGNED).toLong,
          pctPfUniqueReadsAligned = g(H.PCT_PF_UQ_READS_ALIGNED).toDouble,
          meanTargetCoverage = g(H.MEAN_TARGET_COVERAGE).toDouble,
          medianTargetCoverage = g(H.MEDIAN_TARGET_COVERAGE).toDouble,
          maxTargetCoverage = g(H.MAX_TARGET_COVERAGE).toDouble,
          pctTargetBases10 = g(H.PCT_TARGET_BASES_10X).toDouble,
          pctTargetBases20 = g(H.PCT_TARGET_BASES_20X).toDouble,
          pctTargetBases30 = g(H.PCT_TARGET_BASES_30X).toDouble,
          pctTargetBases40 = g(H.PCT_TARGET_BASES_40X).toDouble,
          pctTargetBases50 = g(H.PCT_TARGET_BASES_50X).toDouble,
          pctUsableBasesOnTarget = g(H.PCT_USABLE_BASES_ON_TARGET).toDouble,
          pctExcDupe = g(H.PCT_EXC_DUPE).toDouble,
          pctExcMapQ = g(H.PCT_EXC_MAPQ).toDouble,
          pctExcBaseQ = g(H.PCT_EXC_BASEQ).toDouble,
          pctExcOverlap = g(H.PCT_EXC_OVERLAP).toDouble,
          pctExcOffTarget = g(H.PCT_EXC_OFF_TARGET).toDouble,
        )

        val lane = laneFromReadGroup(g(H.READ_GROUP))
        val runId = runIdFromReadGroup(g(H.READ_GROUP))
        val sampleIdInFile = g(H.SAMPLE)

        require(
          project + "." + sampleId == sampleIdInFile,
          s"unexpected project and sampleId. picard output file has $sampleIdInFile we have prj:$project and sample: $sampleId"
        )

        Root(metrics = metrics,
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
