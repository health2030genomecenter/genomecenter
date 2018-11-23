package org.gc.pipelines.application

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import java.io.File

import org.gc.pipelines.model._
import org.gc.pipelines.application.dto.RunConfigurationDTO

case class DemultiplexingConfiguration(
    sampleSheet: String,
    demultiplexingId: DemultiplexingId,
    /* Mapping between members of a read pair and numbers assigned by bcl2fastq */
    readAssignment: (Int, Int),
    /* Number assigned by bcl2fastq, if any */
    umi: Option[Int],
    extraBcl2FastqArguments: Seq[String]
)

case class RunConfiguration(
    automatic: Boolean,
    demultiplexingRuns: Set[DemultiplexingConfiguration],
    referenceFasta: String,
    targetIntervals: String,
    bqsrKnownSites: Set[String],
    wesSelector: Selector,
    rnaSelector: Selector,
    globalIndexSet: Option[String],
    geneModelGtf: String,
    dbSnpVcf: String,
    variantEvaluationIntervals: String,
    vqsrMillsAnd1Kg: String,
    vqsrHapmap: String,
    vqsrOmni: String,
    vqsrOneKg: String
)

case class RunfolderReadyForProcessing(runId: RunId,
                                       runFolderPath: String,
                                       runConfiguration: RunConfiguration) {
  def isValid = {
    new File(runFolderPath).canRead &&
    new File(runFolderPath, "RunInfo.xml").canRead
  }
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
