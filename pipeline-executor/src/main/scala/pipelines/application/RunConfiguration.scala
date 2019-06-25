package org.gc.pipelines.application

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import java.io.File

import org.gc.pipelines.model._
import org.gc.pipelines.util.{StableSet, sequenceEither}
import org.gc.pipelines.util.Config.option
import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters._
import scala.util.Try
import org.gc.pipelines.util.StableSet.syntax

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

sealed trait AnalysisConfiguration {
  def analysisId: AnalysisId
  def files: Set[String]
  def validationErrors: List[String] =
    files.toList
      .filterNot(path => new File(path).canRead)
      .map(path => s"Can't read $path")
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
    variantCallingContigs: Option[String],
    singleSampleVqsr: Option[Boolean],
    keepVcf: Option[Boolean],
    mergeSingleCalls: Option[Boolean]
) extends AnalysisConfiguration {

  def ignoreMinimumCoverage =
    copy(minimumTargetCoverage = None, minimumWGSCoverage = None)

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

case class TenXConfiguration(
    analysisId: AnalysisId
) extends AnalysisConfiguration {
  def files = Set()
}

case class RNASeqConfiguration(
    analysisId: AnalysisId,
    referenceFasta: String,
    geneModelGtf: String,
    qtlToolsCommandLineArguments: Seq[String],
    quantificationGtf: String,
    starVersion: Option[String]
) extends AnalysisConfiguration {
  def files = Set(referenceFasta, geneModelGtf, quantificationGtf)
}

case class AnalysisAssignments(
    assignments: Map[Project, Seq[AnalysisConfiguration]]
) {
  def assigned(project: Project, conf: AnalysisConfiguration) = {
    val updatedMap = assignments.get(project) match {
      case None => assignments.updated(project, Seq(conf))
      case Some(list) =>
        assignments.updated(
          project,
          (list.filterNot(_.analysisId == conf.analysisId) :+ conf).distinct)
    }
    copy(updatedMap)
  }
  def unassigned(project: Project, analysisId: AnalysisId) = {
    val updated = assignments.map {
      case (project1, list) =>
        if (project1 == project)
          (project, list.filterNot(_.analysisId == analysisId))
        else (project1, list)
    }
    copy(updated)
  }
}

object AnalysisAssignments {
  /* Helper for serialization
   * Circe had codecs for string keyed maps only
   */
  private case class Helper(assignmens: Map[String, Seq[AnalysisConfiguration]])
  val empty = AnalysisAssignments(Map.empty)
  implicit val encoder: Encoder[AnalysisAssignments] =
    deriveEncoder[Helper].contramap(a =>
      Helper(a.assignments.map { case (k, v) => k.toString -> v }))
  implicit val decoder: Decoder[AnalysisAssignments] =
    deriveDecoder[Helper].map { h =>
      AnalysisAssignments(h.assignmens.map { case (k, v) => Project(k) -> v })
    }
}

case class RunConfiguration(
    demultiplexingRuns: StableSet[DemultiplexingConfiguration],
    globalIndexSet: Option[String],
    lastRunOfSamples: StableSet[(Project, SampleId)]
) {
  def isLastRunOfSample(project: Project, sample: SampleId) =
    lastRunOfSamples.contains((project, sample))
  def files =
    demultiplexingRuns.toSeq
      .map(conf => conf.sampleSheet)
      .toSet

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
                             umi: Option[String],
                             read1Length: Option[Int],
                             read2Length: Option[Int],
                             umiLength: Option[Int])

case class RunfolderReadyForProcessing(
    runId: RunId,
    runFolderPath: Option[String],
    demultiplexedSamples: Option[Seq[InputSampleAsFastQ]],
    runConfiguration: RunConfiguration) {

  def sampleSheets =
    runConfiguration.demultiplexingRuns.map(_.sampleSheet).toSeq.map {
      sampleSheetFile =>
        val sampleSheetContents =
          fileutils.openSource(sampleSheetFile)(_.mkString)
        (SampleSheet(sampleSheetContents).parsed, sampleSheetFile)
    }

  def projects = {
    val projectsInSampleSheet =
      runConfiguration.demultiplexingRuns.map(_.sampleSheet).toSeq.flatMap {
        sampleSheetFile =>
          val sampleSheetContents =
            fileutils.openSource(sampleSheetFile)(_.mkString)
          SampleSheet(sampleSheetContents).parsed.poolingLayout
            .map(_.project)
            .distinct
      }
    val projectsInFastqs =
      demultiplexedSamples.toSeq.flatten.map(_.project).distinct

    (projectsInSampleSheet ++ projectsInFastqs).distinct
  }

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

    val demultiplexingIdNonUnique = {
      val error = runConfiguration.demultiplexingRuns
        .map(_.demultiplexingId)
        .size != runConfiguration.demultiplexingRuns.size
      if (error) List("Demultiplexing ids are not unique")
      else Nil
    }

    (unreadableRunfolder ++
      unreadableConfigurationFiles ++
      unreadableFastqs)
      .map(path => s"Can't read: $path") ++ sampleSheetErrors ++ demultiplexingIdNonUnique
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
    RunConfiguration(runConfigurationFile).map(
      runConfiguration =>
        RunfolderReadyForProcessing(RunId(runId),
                                    Some(runFolder.getAbsolutePath),
                                    None,
                                    runConfiguration))
  }

  def fromConfig(
      config: Config): Either[String, RunfolderReadyForProcessing] = {

    val runConfiguration = RunConfiguration(config)

    if (config.hasPath("runFolder")) {
      val runFolder = new File(config.getString("runFolder"))
      val runId = RunId(runFolder.getName)
      runConfiguration.map { runConfiguration =>
        RunfolderReadyForProcessing(runId,
                                    Some(runFolder.getAbsolutePath),
                                    None,
                                    runConfiguration)
      }
    } else {
      val runId = Try(RunId(config.getString("runId"))).toEither.left
        .map(_.toString)
      val demultiplexedSamples =
        for {
          configList <- Try(
            config
              .getConfigList("fastqs")
              .asScala).toEither.left
            .map(e => "Missing fastq configuration: " + e.toString)
          parsed <- {
            val eithers = configList.map(c => InputSampleAsFastQ(c))

            sequenceEither(eithers).left.map(_.mkString(";"))
          }
        } yield parsed

      for {
        runId <- runId
        runConfiguration <- runConfiguration
        demultiplexedSamples <- demultiplexedSamples
      } yield
        RunfolderReadyForProcessing(runId,
                                    None,
                                    Some(demultiplexedSamples),
                                    runConfiguration)
    }

  }
}

object TenXConfiguration {
  implicit val encoder: Encoder[TenXConfiguration] =
    deriveEncoder[TenXConfiguration]
  implicit val decoder: Decoder[TenXConfiguration] =
    deriveDecoder[TenXConfiguration]

  def fromConfig(config: Config) = TenXConfiguration(
    analysisId = AnalysisId(config.getString("analysisId"))
  )

}
object RNASeqConfiguration {
  implicit val encoder: Encoder[RNASeqConfiguration] =
    deriveEncoder[RNASeqConfiguration]
  implicit val decoder: Decoder[RNASeqConfiguration] =
    deriveDecoder[RNASeqConfiguration]

  def fromConfig(config: Config) = RNASeqConfiguration(
    analysisId = AnalysisId(config.getString("analysisId")),
    referenceFasta = config.getString("referenceFasta"),
    geneModelGtf = config.getString("geneModelGtf"),
    qtlToolsCommandLineArguments =
      config.getStringList("qtlToolsCommandLineArguments").asScala.toList,
    quantificationGtf = config.getString("quantificationGtf"),
    starVersion = option(config, "starVersion")(c => p => c.getString(p))
  )

}
object WESConfiguration {
  implicit val encoder: Encoder[WESConfiguration] =
    deriveEncoder[WESConfiguration]
  implicit val decoder: Decoder[WESConfiguration] =
    deriveDecoder[WESConfiguration]

  def fromConfig(config: Config) = WESConfiguration(
    analysisId = AnalysisId(config.getString("analysisId")),
    referenceFasta = config.getString("referenceFasta"),
    targetIntervals = config.getString("targetIntervals"),
    bqsrKnownSites =
      config.getStringList("bqsr.knownSites").asScala.toSet.toStable,
    dbSnpVcf = config.getString("dbSnpVcf"),
    variantEvaluationIntervals = config.getString("variantEvaluationIntervals"),
    vqsrMillsAnd1Kg =
      option(config, "vqsrMillsAnd1Kg")(c => p => c.getString(p)),
    vqsrHapmap = option(config, "vqsrHapmap")(c => p => c.getString(p)),
    vqsrOneKgOmni = option(config, "vqsrOneKgOmni")(c => p => c.getString(p)),
    vqsrOneKgHighConfidenceSnps =
      option(config, "vqsrOneKgHighConfidenceSnps")(c => p => c.getString(p)),
    vqsrDbSnp138 = option(config, "vqsrDbSnp138")(c => p => c.getString(p)),
    doVariantCalls = option(config, "variantCalls")(c => p => c.getBoolean(p)),
    doJointCalls = option(config, "jointCalls")(c => p => c.getBoolean(p)),
    minimumWGSCoverage =
      option(config, "minimumWGSCoverage")(c => p => c.getDouble(p)),
    minimumTargetCoverage =
      option(config, "minimumTargetCoverage")(c => p => c.getDouble(p)),
    variantCallingContigs =
      option(config, "variantCallingContigs")(c => p => c.getString(p)),
    singleSampleVqsr =
      option(config, "singleSampleVqsr")(c => p => c.getBoolean(p)),
    keepVcf = option(config, "keepVcf")(c => p => c.getBoolean(p)),
    mergeSingleCalls =
      option(config, "mergeSingleCalls")(c => p => c.getBoolean(p))
  )
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

  def apply(file: File): Either[String, RunConfiguration] =
    apply(fileutils.openSource(file)(_.mkString))

  def apply(s: String): Either[String, RunConfiguration] =
    RunConfiguration(ConfigFactory.parseString(s))

  def apply(config: Config): Either[String, RunConfiguration] =
    scala.util
      .Try {

        def getDemultiplexings(config: Config) = DemultiplexingConfiguration(
          sampleSheet = config.getString("sampleSheet"),
          demultiplexingId = DemultiplexingId(config.getString("id")),
          readAssignment = {
            val list = config.getIntList("readAssignment").asScala
            if (list.size != 2)
              throw new RuntimeException("readAssignment needs two numbers")
            (list(0), list(1))
          },
          umi =
            config.getIntList("umiReadNumber").asScala.headOption.map(_.toInt),
          extraBcl2FastqArguments =
            config.getStringList("extraBcl2FastqArguments").asScala,
          tenX =
            if (config.hasPath("tenX")) Some(config.getBoolean("tenX"))
            else None,
          partitionByLane =
            if (config.hasPath("partitionByLane"))
              Some(config.getBoolean("partitionByLane"))
            else None,
          partitionByTileCount =
            if (config.hasPath("partitionByTileCount"))
              Some(config.getInt("partitionByTileCount"))
            else None
        )

        RunConfiguration(
          demultiplexingRuns = config
            .getConfigList("demultiplexing")
            .asScala
            .map(getDemultiplexings)
            .toSet
            .toStable,
          globalIndexSet =
            if (config.hasPath("globalIndexSet"))
              Some(config.getString("globalIndexSet"))
            else None,
          lastRunOfSamples =
            if (config.hasPath("lastRunOfSamples"))
              config
                .getStringList("lastRunOfSamples")
                .asScala
                .grouped(2)
                .map(group => (Project(group(0)), SampleId(group(1))))
                .toSet
                .toStable
            else Set.empty.toStable
        )

      }
      .toEither
      .left
      .map(e => "Failed to parse RunConfiguration: " + e.toString)

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
    val read1Length = option(config, "read1Length")(c => p => c.getInt(p))
    val read2Length = option(config, "read2Length")(c => p => c.getInt(p))
    val umiLength = option(config, "umiLength")(c => p => c.getInt(p))
    InputFastQPerLane(Lane(lane),
                      read1,
                      read2,
                      umi,
                      read1Length,
                      read2Length,
                      umiLength)
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
    }).toEither.left.map(e => "Failed parsing fastq entries: " + e.toString)

}

object AnalysisConfiguration {
  implicit val encoder: Encoder[AnalysisConfiguration] =
    deriveEncoder[AnalysisConfiguration]
  implicit val decoder: Decoder[AnalysisConfiguration] =
    deriveDecoder[AnalysisConfiguration]

  def fromConfig(config: Config) = {
    val rna =
      if (config.hasPath("rna"))
        Some(
          Try(RNASeqConfiguration.fromConfig(config.getConfig("rna"))).toEither.left
            .map(_.toString))
      else None
    val wes =
      if (config.hasPath("wes"))
        Some(
          Try(WESConfiguration.fromConfig(config.getConfig("wes"))).toEither.left
            .map(_.toString))
      else None

    val tenX =
      if (config.hasPath("tenX"))
        Some(
          Try(TenXConfiguration.fromConfig(config.getConfig("tenX"))).toEither.left
            .map(_.toString))
      else None

    rna.getOrElse(
      wes.getOrElse(
        tenX.getOrElse(Left("expected 'rna' or 'wes' or 'tenX' objects"))))
  }

}
