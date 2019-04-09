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
) extends WithSharedFiles(
      dbSnpVcf.files ++ vqsrTrainingFiles.toSeq
        .flatMap(_.files) ++ variantCallingContigs.toSeq.flatMap(_.files): _*)

case class SampleResult(
    wes: Seq[(SingleSamplePipelineResult, SingleSampleConfiguration)],
    rna: Seq[SingleSamplePipelineResultRNA],
    demultiplexed: Seq[PerSamplePerRunFastQ],
    fastpReports: Seq[FastpReport],
    runFolders: Seq[RunfolderReadyForProcessing],
    project: Project,
    sampleId: SampleId
) extends WithSharedFiles(
      wes.toSeq.flatMap(_._1.files) ++
        wes.toSeq.flatMap(_._2.files) ++
        rna.toSeq.flatMap(_.files) ++
        demultiplexed.flatMap(_.files) ++
        fastpReports.flatMap(_.files): _*
    ) {
  def lastRunId = runFolders.last.runId

  def extractWESQCFiles: Seq[SampleMetrics] =
    wes
      .flatMap {
        case (sample, sampleConfig) =>
          sample.mergedRuns.map { mergedRuns =>
            (mergedRuns, sampleConfig)
          }
      }
      .map {
        case (sample, sampleConfig) =>
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
            sample.alignmentQC.insertSizeMetrics
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
    quantificationGtf: GTFFile
) extends WithSharedFiles(
      demultiplexed.files ++ reference.files ++ gtf.files: _*)

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
    vqsrTrainingFiles: Option[VQSRTrainingFiles])
    extends WithSharedFiles(demultiplexed.files ++ reference.files ++ knownSites
      .flatMap(_.files) ++ selectionTargetIntervals.files ++ alignedLanes.toSeq
      .flatMap(_.files): _*)

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
    extends WithSharedFiles(
      bam.files ++ alignmentQC.files ++ duplicationQC.files ++ targetSelectionQC.files ++ wgsQC.files ++ haplotypeCallerReferenceCalls.toSeq
        .flatMap(_.files) ++ gvcf.toSeq
        .flatMap(_.files) ++ referenceFasta.files ++ gvcfQCOverall.toSeq
        .flatMap(_.files) ++ gvcfQCInterval.toSeq
        .flatMap(_.files): _*)

case class SingleSamplePipelineResult(
    alignedLanes: StableSet[BamWithSampleMetadataPerLane],
    mergedRuns: Option[PerSampleMergedWESResult],
    coverage: MeanCoverageResult
) // mergedRuns is unchecked to allow overwriting
    extends WithSharedFiles(alignedLanes.toSeq.flatMap(_.files): _*)

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
