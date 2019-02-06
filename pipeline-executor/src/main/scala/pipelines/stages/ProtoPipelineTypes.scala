package org.gc.pipelines.stages

import tasks._
import org.gc.pipelines.application.{
  RunfolderReadyForProcessing,
  WESConfiguration
}

import org.gc.pipelines.model._
import org.gc.pipelines.util.StableSet
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class SampleResult(
    wes: Seq[SingleSamplePipelineResult],
    rna: Seq[SingleSamplePipelineResultRNA],
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

  def extractWESQCFiles: Seq[SampleMetrics] =
    wes.map { sample =>
      val fastpReportsOfSample = fastpReports.find { fp =>
        fp.sampleId == sample.sampleId &&
        fp.project == sample.project
      }.get
      SampleMetrics(
        sample.analysisId,
        sample.alignmentQC.alignmentSummary,
        sample.targetSelectionQC.hsMetrics,
        sample.duplicationQC.markDuplicateMetrics,
        fastpReportsOfSample,
        sample.wgsQC.wgsMetrics,
        sample.gvcfQC.map(_.summary),
        sample.project,
        sample.sampleId,
        sample.alignmentQC.insertSizeMetrics
      )
    }
}

case class SingleSamplePipelineInputRNASeq(
    analysisId: AnalysisId,
    demultiplexed: PerSampleFastQ,
    reference: ReferenceFasta,
    gtf: GTFFile,
    readLengths: StableSet[(ReadType, Int)])
    extends WithSharedFiles(
      demultiplexed.files ++ reference.files ++ gtf.files: _*)

case class SingleSamplePipelineInput(analysisId: AnalysisId,
                                     demultiplexed: PerSampleFastQ,
                                     reference: ReferenceFasta,
                                     knownSites: StableSet[VCF],
                                     selectionTargetIntervals: BedFile,
                                     dbSnpVcf: VCF,
                                     variantEvaluationIntervals: BedFile,
                                     bamOfPreviousRuns: Option[Bam],
                                     doVariantCalling: Boolean,
                                     minimumWGSCoverage: Option[Double],
                                     minimumTargetCoverage: Option[Double])
    extends WithSharedFiles(demultiplexed.files ++ reference.files ++ knownSites
      .flatMap(_.files) ++ selectionTargetIntervals.files ++ bamOfPreviousRuns.toSeq
      .flatMap(_.files): _*)

case class SingleSamplePipelineResult(
    bam: CoordinateSortedBam,
    uncalibrated: Bam,
    haplotypeCallerReferenceCalls: Option[VCF],
    gvcf: Option[VCF],
    project: Project,
    sampleId: SampleId,
    alignmentQC: AlignmentQCResult,
    duplicationQC: DuplicationQCResult,
    targetSelectionQC: SelectionQCResult,
    wgsQC: CollectWholeGenomeMetricsResult,
    gvcfQC: Option[VariantCallingMetricsResult],
    analysisId: AnalysisId,
    referenceFasta: IndexedReferenceFasta,
    dbSnpVcf: VCF,
    vqsrTrainingFiles: Option[VQSRTrainingFiles],
    wesConfiguration: Option[WESConfiguration])
    extends WithSharedFiles(
      bam.files ++ alignmentQC.files ++ duplicationQC.files ++ targetSelectionQC.files ++ wgsQC.files ++ haplotypeCallerReferenceCalls.toSeq
        .flatMap(_.files) ++ referenceFasta.files ++ dbSnpVcf.files ++ vqsrTrainingFiles.toSeq
        .flatMap(_.files) ++ gvcf.toSeq.flatMap(_.files) ++ gvcfQC.toSeq
        .flatMap(_.files): _*)

case class SingleSamplePipelineResultRNA(
    analysisId: AnalysisId,
    star: StarResult,
    quantification: QTLToolsQuantificationResult
) extends WithSharedFiles(star.files ++ quantification.files: _*)

case class PerSamplePipelineResultRNASeq(
    samples: StableSet[SingleSamplePipelineResultRNA])
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
