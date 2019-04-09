package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.gc.pipelines.util.{Fasta, Bed, MappedBases}
import scala.collection.JavaConverters._
import org.gc.pipelines.model._
import akka.stream.scaladsl.Source
import akka.util.ByteString

case class BamCoverageInput(
    bases: List[CountMappedBasesResult],
    reference: IndexedReferenceFasta,
    selectionTargetIntervals: BedFile
) extends WithSharedFiles(
      reference.files ++ selectionTargetIntervals.files: _*)
case class CountMappedBasesInput(
    bam: Bam,
    selectionTargetIntervals: BedFile
) extends WithSharedFiles(bam.files ++ selectionTargetIntervals.files: _*)

case class CountMappedBasesResult(all: Long, target: Long)
case class MeanCoverageResult(all: Double, target: Double) {

  def reachedCoverageTarget(minimumTargetedCoverage: Option[Double],
                            minimumWGSCoverage: Option[Double]) =
    minimumWGSCoverage.forall(_ <= all) &&
      minimumTargetedCoverage.forall(_ <= target)

}

case class WriteCoverageInput(
    qc: MeanCoverageResult,
    runId: String,
    project: Project,
    sample: SampleId,
    analysisId: AnalysisId
) extends WithSharedFiles

object FastCoverage {

  val computeCoverage =
    AsyncTask[BamCoverageInput, MeanCoverageResult]("__compute_coverage", 1) {
      case BamCoverageInput(bases, reference, selectionTargetIntervals) =>
        implicit computationEnvironment =>
          for {
            dict <- reference.dict
            bed <- selectionTargetIntervals.file.file

          } yield {
            val total = Fasta
              .parseDict(dict)
              .getSequences
              .asScala
              .toList
              .map { sequence =>
                sequence.getSequenceLength
              }
              .sum
            val totalTarget = Bed.size(bed)
            val covered = bases.map(_.all).sum
            val coveredTarget = bases.map(_.target).sum
            val mean = covered.toDouble / total
            val meanTarget = coveredTarget.toDouble / totalTarget
            MeanCoverageResult(mean, meanTarget)
          }
    }

  val countMappedBases =
    AsyncTask[CountMappedBasesInput, CountMappedBasesResult](
      "__count_mapped_bases",
      1) {
      case CountMappedBasesInput(bam, target) =>
        implicit computationEnvironment =>
          for {
            bam <- bam.file.file
            target <- target.file.file
          } yield {

            val (bases, targetBases) = MappedBases.countBases(
              file = bam,
              minimumMapQ = 20,
              minimumReadLength = 30,
              bedFile = Some(target.getAbsolutePath)
            )

            CountMappedBasesResult(bases, targetBases)

          }
    }

  val writeCoverageToFile =
    AsyncTask[WriteCoverageInput, SharedFile]("__wgs_coverage", 1) {
      case WriteCoverageInput(coverage,
                              runIdTag,
                              project,
                              sample,
                              analysisId) =>
        implicit computationEnvironment =>
          for {
            sf <- SharedFile(
              Source.single(ByteString(coverage.all.toString)),
              project + "." + sample + "." + analysisId + "." + runIdTag)
          } yield sf
    }
}

object MeanCoverageResult {
  implicit val encoder: Encoder[MeanCoverageResult] =
    deriveEncoder[MeanCoverageResult]
  implicit val decoder: Decoder[MeanCoverageResult] =
    deriveDecoder[MeanCoverageResult]
}
object BamCoverageInput {
  implicit val encoder: Encoder[BamCoverageInput] =
    deriveEncoder[BamCoverageInput]
  implicit val decoder: Decoder[BamCoverageInput] =
    deriveDecoder[BamCoverageInput]
}

object CountMappedBasesInput {
  implicit val encoder: Encoder[CountMappedBasesInput] =
    deriveEncoder[CountMappedBasesInput]
  implicit val decoder: Decoder[CountMappedBasesInput] =
    deriveDecoder[CountMappedBasesInput]
}
object CountMappedBasesResult {
  implicit val encoder: Encoder[CountMappedBasesResult] =
    deriveEncoder[CountMappedBasesResult]
  implicit val decoder: Decoder[CountMappedBasesResult] =
    deriveDecoder[CountMappedBasesResult]
}
object WriteCoverageInput {
  implicit val encoder: Encoder[WriteCoverageInput] =
    deriveEncoder[WriteCoverageInput]
  implicit val decoder: Decoder[WriteCoverageInput] =
    deriveDecoder[WriteCoverageInput]
}
