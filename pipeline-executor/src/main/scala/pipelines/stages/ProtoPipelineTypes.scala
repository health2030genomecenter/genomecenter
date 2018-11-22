package org.gc.pipelines.stages

import tasks._
import org.gc.pipelines.application.RunfolderReadyForProcessing

import org.gc.pipelines.model._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class SampleResult(
    wes: Option[SingleSamplePipelineResult],
    rna: Option[SingleSamplePipelineResultRNA],
    demultiplexed: Seq[PerSamplePerRunFastQ],
    fastpReports: Seq[FastpReport],
    runFolders: Seq[RunfolderReadyForProcessing],
    project: Project,
    sampleId: SampleId
) extends WithSharedFiles(
      wes.toSeq.flatMap(_.files) ++
        rna.toSeq.flatMap(_.files) ++
        demultiplexed.flatMap(_.files) ++
        fastpReports.flatMap(_.files): _*
    ) {
  def lastRunId = runFolders.last.runId

  def extractWESQCFiles: Option[SampleMetrics] =
    wes.map { sample =>
      val fastpReportsOfSample = fastpReports.find { fp =>
        fp.sampleId == sample.sampleId &&
        fp.project == sample.project
      }.get
      SampleMetrics(
        sample.alignmentQC.alignmentSummary,
        sample.targetSelectionQC.hsMetrics,
        sample.duplicationQC.markDuplicateMetrics,
        fastpReportsOfSample,
        sample.wgsQC.wgsMetrics,
        sample.gvcfQC.summary,
        sample.project,
        sample.sampleId
      )
    }
}

case class SingleSamplePipelineInputRNASeq(demultiplexed: PerSampleFastQ,
                                           reference: ReferenceFasta,
                                           gtf: GTFFile,
                                           readLengths: Set[(ReadType, Int)])
    extends WithSharedFiles(
      demultiplexed.files ++ reference.files ++ gtf.files: _*)

case class SingleSamplePipelineInput(demultiplexed: PerSampleFastQ,
                                     reference: ReferenceFasta,
                                     knownSites: Set[VCF],
                                     selectionTargetIntervals: BedFile,
                                     dbSnpVcf: VCF,
                                     variantEvaluationIntervals: BedFile,
                                     bamOfPreviousRuns: Option[Bam],
                                     vqsrTrainingFiles: VQSRTrainingFiles)
    extends WithSharedFiles(demultiplexed.files ++ reference.files ++ knownSites
      .flatMap(_.files) ++ selectionTargetIntervals.files ++ bamOfPreviousRuns.toSeq
      .flatMap(_.files) ++ vqsrTrainingFiles.files: _*)

case class SingleSamplePipelineResult(bam: CoordinateSortedBam,
                                      uncalibrated: Bam,
                                      haplotypeCallerReferenceCalls: VCF,
                                      gvcf: VCF,
                                      project: Project,
                                      sampleId: SampleId,
                                      alignmentQC: AlignmentQCResult,
                                      duplicationQC: DuplicationQCResult,
                                      targetSelectionQC: SelectionQCResult,
                                      wgsQC: CollectWholeGenomeMetricsResult,
                                      gvcfQC: VariantCallingMetricsResult)
    extends WithSharedFiles(
      bam.files ++ alignmentQC.files ++ duplicationQC.files ++ targetSelectionQC.files ++ wgsQC.files ++ haplotypeCallerReferenceCalls.files ++ gvcf.files: _*)

case class SingleSamplePipelineResultRNA(
    star: StarResult
) extends WithSharedFiles(star.files: _*)

case class PerSamplePipelineResultRNASeq(
    samples: Set[SingleSamplePipelineResultRNA])
    extends WithSharedFiles(samples.toSeq.flatMap(_.files): _*)

object SingleSamplePipelineInput {
  implicit val encoder: Encoder[SingleSamplePipelineInput] =
    deriveEncoder[SingleSamplePipelineInput]
  implicit val decoder: Decoder[SingleSamplePipelineInput] =
    deriveDecoder[SingleSamplePipelineInput]
}

object SingleSamplePipelineResult {
  implicit val encoder: Encoder[SingleSamplePipelineResult] =
    deriveEncoder[SingleSamplePipelineResult]
  implicit val decoder: Decoder[SingleSamplePipelineResult] =
    deriveDecoder[SingleSamplePipelineResult]
}

object SingleSamplePipelineInputRNASeq {
  implicit val encoder: Encoder[SingleSamplePipelineInputRNASeq] =
    deriveEncoder[SingleSamplePipelineInputRNASeq]
  implicit val decoder: Decoder[SingleSamplePipelineInputRNASeq] =
    deriveDecoder[SingleSamplePipelineInputRNASeq]
}

object PerSamplePipelineResultRNASeq {
  implicit val encoder: Encoder[PerSamplePipelineResultRNASeq] =
    deriveEncoder[PerSamplePipelineResultRNASeq]
  implicit val decoder: Decoder[PerSamplePipelineResultRNASeq] =
    deriveDecoder[PerSamplePipelineResultRNASeq]
}

object SingleSamplePipelineResultRNA {
  implicit val encoder: Encoder[SingleSamplePipelineResultRNA] =
    deriveEncoder[SingleSamplePipelineResultRNA]
  implicit val decoder: Decoder[SingleSamplePipelineResultRNA] =
    deriveDecoder[SingleSamplePipelineResultRNA]
}

object SampleResult {
  implicit val encoder: Encoder[SampleResult] =
    deriveEncoder[SampleResult]
  implicit val decoder: Decoder[SampleResult] =
    deriveDecoder[SampleResult]
}
