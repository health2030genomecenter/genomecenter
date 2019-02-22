package org.gc.pipelines.application

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import java.io.File

import org.gc.pipelines.model._
import org.gc.pipelines.application.dto.RunConfigurationDTO
import org.gc.pipelines.util.{StableSet, sequenceEither}
import com.typesafe.config.Config
import scala.collection.JavaConverters._
import scala.util.Try

case class DemultiplexingConfiguration(
    sampleSheet: String,
    demultiplexingId: DemultiplexingId,
    /* Mapping between members of a read pair and numbers assigned by bcl2fastq */
    readAssignment: (Int, Int),
    /* Number assigned by bcl2fastq, if any */
    umi: Option[Int],
    extraBcl2FastqArguments: Seq[String],
    tenX: Option[Boolean],
    partitionByLane: Option[Boolean],
    partitionByTileCount: Option[Int]
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
    vqsrDbSnp138: Option[String],
    doVariantCalls: Option[Boolean],
    doJointCalls: Option[Boolean],
    minimumWGSCoverage: Option[Double],
    minimumTargetCoverage: Option[Double],
    variantCallingContigs: Option[String]
) {
  def files =
    Set(referenceFasta, targetIntervals, dbSnpVcf, variantEvaluationIntervals) ++ bqsrKnownSites.toSeq.toSet ++ vqsrMillsAnd1Kg.toSet ++ vqsrHapmap.toSet ++ vqsrOneKgHighConfidenceSnps.toSet ++ vqsrOneKgOmni.toSet ++ vqsrDbSnp138.toSet ++ variantCallingContigs.toSet ++
      vqsrMillsAnd1Kg.map((_: String) + ".tbi").toSet ++ vqsrHapmap
      .map((_: String) + ".tbi")
      .toSet ++ vqsrOneKgOmni
      .map((_: String) + ".tbi")
      .toSet ++ vqsrOneKgHighConfidenceSnps
      .map((_: String) + ".tbi")
      .toSet ++ vqsrDbSnp138.map((_: String) + ".tbi").toSet ++ Set(
      dbSnpVcf + ".tbi") ++
      bqsrKnownSites.toSeq.map((_: String) + ".tbi").toSet
}

case class RNASeqConfiguration(
    analysisId: AnalysisId,
    referenceFasta: String,
    geneModelGtf: String,
    qtlToolsCommandLineArguments: Seq[String],
    quantificationGtf: String
) {
  def files = Set(referenceFasta, geneModelGtf, quantificationGtf)
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

case class InputSampleAsFastQ(
    lanes: Set[InputFastQPerLane],
    project: Project,
    sampleId: SampleId
) {
  def files =
    lanes.toSeq
      .flatMap(l => List(l.read1Path, l.read2Path) ++ l.umi.toList)
      .toSet
}

case class InputFastQPerLane(lane: Lane,
                             read1Path: String,
                             read2Path: String,
                             umi: Option[String])

case class RunfolderReadyForProcessing(
    runId: RunId,
    runFolderPath: Option[String],
    demultiplexedSamples: Option[Seq[InputSampleAsFastQ]],
    runConfiguration: RunConfiguration) {

  private def unreadableFiles(files: Set[String]) =
    files.filterNot(s => new File(s).canRead)

  def validationErrors: List[String] = {

    val unreadableRunfolder = runFolderPath match {
      case None => Nil
      case Some(runFolderPath) =>
        unreadableFiles(
          Set(runFolderPath,
              new File(runFolderPath, "RunInfo.xml").getAbsolutePath)).toList
    }

    val unreadableConfigurationFiles = unreadableFiles(runConfiguration.files).toList

    val unreadableFastqs = unreadableFiles(
      demultiplexedSamples.toSeq.flatten.flatMap(_.files).toSet).toList

    val sampleSheetErrors = runConfiguration.demultiplexingRuns.toSeq.flatMap {
      dm =>
        if (new File(dm.sampleSheet).canRead) {
          val parsedSampleSheet = fileutils.openSource(dm.sampleSheet)(s =>
            SampleSheet(s.mkString).parsed)
          parsedSampleSheet.validationErrors
        } else List(s"Can't read ${dm.sampleSheet}")
    }

    (unreadableRunfolder ++
      unreadableConfigurationFiles ++
      unreadableFastqs).map(path => s"Can't read: $path") ++ sampleSheetErrors
  }

}

object RunfolderReadyForProcessing {
  implicit val encoder: Encoder[RunfolderReadyForProcessing] =
    deriveEncoder[RunfolderReadyForProcessing]
  implicit val decoder: Decoder[RunfolderReadyForProcessing] =
    deriveDecoder[RunfolderReadyForProcessing]

  def readFolderWithConfigFile(runFolder: File, runConfigurationFile: File)
    : Either[String, RunfolderReadyForProcessing] = {

    val runId = runFolder.getAbsoluteFile.getName
    RunConfigurationDTO(runConfigurationFile).map(
      runConfigurationDTO =>
        RunfolderReadyForProcessing(RunId(runId),
                                    Some(runFolder.getAbsolutePath),
                                    None,
                                    runConfigurationDTO.toRunConfiguration))
  }

  def fromConfig(
      runConfiguration: Config): Either[String, RunfolderReadyForProcessing] = {

    val runConfigurationDTO = RunConfigurationDTO(runConfiguration)

    if (runConfiguration.hasPath("runFolder")) {
      val runFolder = new File(runConfiguration.getString("runFolder"))
      val runId = RunId(runFolder.getName)
      runConfigurationDTO.map { dto =>
        RunfolderReadyForProcessing(runId,
                                    Some(runFolder.getAbsolutePath),
                                    None,
                                    dto.toRunConfiguration)
      }
    } else {
      val runId = Try(RunId(runConfiguration.getString("runId"))).toEither.left
        .map(_.toString)
      val demultiplexedSamples =
        for {
          configList <- Try(
            runConfiguration
              .getConfigList("fastqs")
              .asScala).toEither.left
            .map(_.toString)
          parsed <- {
            val eithers = configList.map(c => InputSampleAsFastQ(c))

            sequenceEither(eithers).left.map(_.mkString(";"))
          }
        } yield parsed

      for {
        runId <- runId
        dto <- runConfigurationDTO
        demultiplexedSamples <- demultiplexedSamples
      } yield
        RunfolderReadyForProcessing(runId,
                                    None,
                                    Some(demultiplexedSamples),
                                    dto.toRunConfiguration)
    }

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
object InputFastQPerLane {
  implicit val encoder: Encoder[InputFastQPerLane] =
    deriveEncoder[InputFastQPerLane]
  implicit val decoder: Decoder[InputFastQPerLane] =
    deriveDecoder[InputFastQPerLane]

  def apply(config: Config): InputFastQPerLane = {
    val lane = config.getInt("lane")
    val read1 = config.getString("read1")
    val read2 = config.getString("read2")
    val umi = if (config.hasPath("umi")) Some(config.getString("umi")) else None
    InputFastQPerLane(Lane(lane), read1, read2, umi)
  }

}
object InputSampleAsFastQ {
  implicit val encoder: Encoder[InputSampleAsFastQ] =
    deriveEncoder[InputSampleAsFastQ]
  implicit val decoder: Decoder[InputSampleAsFastQ] =
    deriveDecoder[InputSampleAsFastQ]

  def apply(config: Config): Either[String, InputSampleAsFastQ] =
    (Try {
      val project = config.getString("project")
      val sampleId = config.getString("sampleId")
      val lanes = config
        .getConfigList("lanes")
        .asScala
        .map(config => InputFastQPerLane(config))
      InputSampleAsFastQ(lanes.toSet, Project(project), SampleId(sampleId))
    }).toEither.left.map(_.toString)

}
