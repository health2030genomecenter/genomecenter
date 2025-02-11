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

case class SingleSampleConfiguration(
    analysisId: AnalysisId,
    dbSnpVcf: VCF,
    vqsrTrainingFiles: Option[VQSRTrainingFiles],
    wesConfiguration: WESConfiguration,
    variantCallingContigs: Option[ContigsFile]
)

case class SampleResult(
    wes: Seq[(SingleSamplePipelineResult,
              SingleSampleConfiguration,
              List[(RunId, MeanCoverageResult)])],
    rna: Seq[SingleSamplePipelineResultRNA],
    tenX: Seq[PerSampleFastQ],
    demultiplexed: Seq[PerSamplePerRunFastQ],
    fastpReports: Seq[FastpReport],
    runFolders: Seq[RunfolderReadyForProcessing],
    project: Project,
    sampleId: SampleId
) {
  def lastRunId = runFolders.last.runId

  def extractWESQCFiles: Seq[SampleMetrics] =
    wes
      .flatMap {
        case (sample, sampleConfig, coverages) =>
          sample.mergedRuns.map { mergedRuns =>
            (mergedRuns, sampleConfig, coverages)
          }
      }
      .map {
        case (sample, sampleConfig, coverages) =>
          val fastpReportsOfSample = fastpReports.find { fp =>
            fp.sampleId == sample.sampleId &&
            fp.project == sample.project
          }.get
          SampleMetrics(
            sampleConfig.analysisId,
            sample.alignmentQC.alignmentSummary,
            sample.targetSelectionQC.hsMetrics,
            sample.duplicationQC.markDuplicateMetrics,
            fastpReportsOfSample,
            sample.wgsQC.wgsMetrics,
            sample.gvcfQCInterval.map(_.summary),
            sample.gvcfQCOverall.map(_.summary),
            sample.project,
            sample.sampleId,
            sample.alignmentQC.insertSizeMetrics,
            coverages
          )
      }
}

case class SingleSamplePipelineInputRNASeq(
    analysisId: AnalysisId,
    demultiplexed: PerSampleFastQ,
    reference: ReferenceFasta,
    gtf: GTFFile,
    readLengths: StableSet[(ReadType, Int)],
    qtlToolsArguments: Seq[String],
    quantificationGtf: GTFFile,
    starVersion: StarVersion
)

case class SingleSamplePipelineInput(
    analysisId: AnalysisId,
    demultiplexed: PerSampleFastQ,
    reference: ReferenceFasta,
    knownSites: StableSet[VCF],
    selectionTargetIntervals: BedFile,
    dbSnpVcf: VCF,
    variantEvaluationIntervals: BedFile,
    alignedLanes: StableSet[BamWithSampleMetadataPerLane],
    doVariantCalling: Boolean,
    minimumWGSCoverage: Option[Double],
    minimumTargetCoverage: Option[Double],
    contigsFile: Option[ContigsFile],
    vqsrTrainingFiles: Option[VQSRTrainingFiles],
    keepVcf: Option[Boolean])

case class PerSampleMergedWESResult(
    bam: CoordinateSortedBam,
    haplotypeCallerReferenceCalls: Option[VCF],
    gvcf: Option[VCF],
    project: Project,
    sampleId: SampleId,
    alignmentQC: AlignmentQCResult,
    duplicationQC: DuplicationQCResult,
    targetSelectionQC: SelectionQCResult,
    wgsQC: CollectWholeGenomeMetricsResult,
    gvcfQCInterval: Option[VariantCallingMetricsResult],
    gvcfQCOverall: Option[VariantCallingMetricsResult],
    referenceFasta: IndexedReferenceFasta)

case class SingleSamplePipelineResult(
    alignedLanes: StableSet[BamWithSampleMetadataPerLane],
    mergedRuns: Option[PerSampleMergedWESResult],
    coverage: MeanCoverageResult
) // mergedRuns is unchecked to allow overwriting
    extends WithSharedFiles(mutables = mergedRuns.toSeq)

case class SingleSamplePipelineResultRNA(
    analysisId: AnalysisId,
    star: StarResult,
    quantification: QTLToolsQuantificationResult
) extends WithSharedFiles(
      mutables = List(star, quantification)
    )

case class PerSamplePipelineResultRNASeq(
    samples: StableSet[SingleSamplePipelineResultRNA])

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

object SingleSampleConfiguration {
  implicit val encoder: Encoder[SingleSampleConfiguration] =
    deriveEncoder[SingleSampleConfiguration]
  implicit val decoder: Decoder[SingleSampleConfiguration] =
    deriveDecoder[SingleSampleConfiguration]
}

object SampleResult {
  implicit val encoder: Encoder[SampleResult] =
    deriveEncoder[SampleResult]
  implicit val decoder: Decoder[SampleResult] =
    deriveDecoder[SampleResult]
}

object PerSampleMergedWESResult {
  implicit val encoder: Encoder[PerSampleMergedWESResult] =
    deriveEncoder[PerSampleMergedWESResult]
  implicit val decoder: Decoder[PerSampleMergedWESResult] =
    deriveDecoder[PerSampleMergedWESResult]
}
