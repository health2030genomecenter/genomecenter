package org.gc.pipelines.application

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import java.io.File

import org.gc.pipelines.model._
import org.gc.pipelines.application.dto.RunConfigurationDTO
import org.gc.pipelines.util.StableSet

case class DemultiplexingConfiguration(
    sampleSheet: String,
    demultiplexingId: DemultiplexingId,
    /* Mapping between members of a read pair and numbers assigned by bcl2fastq */
    readAssignment: (Int, Int),
    /* Number assigned by bcl2fastq, if any */
    umi: Option[Int],
    extraBcl2FastqArguments: Seq[String],
    tenX: Option[Boolean],
    partitionByLane: Option[Boolean]
) {
  def isTenX = tenX.exists(identity)
}

case class WESConfiguration(
    analysisId: AnalysisId,
    referenceFasta: String,
    targetIntervals: String,
    bqsrKnownSites: StableSet[String],
    dbSnpVcf: String,
    variantEvaluationIntervals: String,
    vqsrMillsAnd1Kg: Option[String],
    vqsrHapmap: Option[String],
    vqsrOneKgOmni: Option[String],
    vqsrOneKgHighConfidenceSnps: Option[String],
    vqsrDbSnp138: Option[String]
) {
  def files =
    Set(referenceFasta, targetIntervals, dbSnpVcf, variantEvaluationIntervals) ++ bqsrKnownSites.toSeq ++ vqsrMillsAnd1Kg.toSet ++ vqsrHapmap.toSet ++ vqsrOneKgHighConfidenceSnps.toSet ++ vqsrOneKgOmni.toSet ++ vqsrDbSnp138.toSet
}

case class RNASeqConfiguration(
    analysisId: AnalysisId,
    referenceFasta: String,
    geneModelGtf: String
) {
  def files = Set(referenceFasta, geneModelGtf)
}

case class RunConfiguration(
    demultiplexingRuns: StableSet[DemultiplexingConfiguration],
    globalIndexSet: Option[String],
    wesProcessing: StableSet[(Selector, WESConfiguration)],
    rnaProcessing: StableSet[(Selector, RNASeqConfiguration)]
) {
  def files = {
    val sampleSheets = demultiplexingRuns.toSeq
      .map(conf => conf.sampleSheet)
    val wesFiles = wesProcessing.toSeq.flatMap(_._2.files.toSeq)
    val rnaFiles = rnaProcessing.toSeq
      .flatMap(_._2.files.toSeq)
    (sampleSheets ++ wesFiles ++ rnaFiles).toSet
  }
}
case class RunfolderReadyForProcessing(runId: RunId,
                                       runFolderPath: String,
                                       runConfiguration: RunConfiguration) {

  private def filesCanRead(files: Set[String]) =
    files.forall(s => new File(s).canRead)

  def isValid =
    new File(runFolderPath).canRead &&
      new File(runFolderPath, "RunInfo.xml").canRead &&
      filesCanRead(runConfiguration.files)

}

object RunfolderReadyForProcessing {
  implicit val encoder: Encoder[RunfolderReadyForProcessing] =
    deriveEncoder[RunfolderReadyForProcessing]
  implicit val decoder: Decoder[RunfolderReadyForProcessing] =
    deriveDecoder[RunfolderReadyForProcessing]

  def readFolder(
      runFolder: File,
      configFileFolder: File): Either[String, RunfolderReadyForProcessing] = {

    val runId = runFolder.getAbsoluteFile.getName
    val runConfigurationFile = new File(configFileFolder, "config-" + runId)
    RunConfigurationDTO(runConfigurationFile).map(
      runConfigurationDTO =>
        RunfolderReadyForProcessing(RunId(runId),
                                    runFolder.getAbsolutePath,
                                    runConfigurationDTO.toRunConfiguration))
  }

  def readFolderWithConfigFile(runFolder: File, runConfigurationFile: File)
    : Either[String, RunfolderReadyForProcessing] = {

    val runId = runFolder.getAbsoluteFile.getName
    RunConfigurationDTO(runConfigurationFile).map(
      runConfigurationDTO =>
        RunfolderReadyForProcessing(RunId(runId),
                                    runFolder.getAbsolutePath,
                                    runConfigurationDTO.toRunConfiguration))
  }
}

object RNASeqConfiguration {
  implicit val encoder: Encoder[RNASeqConfiguration] =
    deriveEncoder[RNASeqConfiguration]
  implicit val decoder: Decoder[RNASeqConfiguration] =
    deriveDecoder[RNASeqConfiguration]
}
object WESConfiguration {
  implicit val encoder: Encoder[WESConfiguration] =
    deriveEncoder[WESConfiguration]
  implicit val decoder: Decoder[WESConfiguration] =
    deriveDecoder[WESConfiguration]
}
object DemultiplexingConfiguration {
  implicit val encoder: Encoder[DemultiplexingConfiguration] =
    deriveEncoder[DemultiplexingConfiguration]
  implicit val decoder: Decoder[DemultiplexingConfiguration] =
    deriveDecoder[DemultiplexingConfiguration]
}

object RunConfiguration {
  implicit val encoder: Encoder[RunConfiguration] =
    deriveEncoder[RunConfiguration]
  implicit val decoder: Decoder[RunConfiguration] =
    deriveDecoder[RunConfiguration]

}
