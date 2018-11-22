package org.gc.pipelines.application

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import java.io.File
import com.typesafe.config.{ConfigFactory, Config}
import scala.collection.JavaConverters._
import org.gc.pipelines.model._

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
    RunConfiguration(runConfigurationFile).map(
      runConfiguration =>
        RunfolderReadyForProcessing(RunId(runId),
                                    runFolder.getAbsolutePath,
                                    runConfiguration))
  }

  def readFolderWithConfigFile(runFolder: File, runConfigurationFile: File)
    : Either[String, RunfolderReadyForProcessing] = {

    val runId = runFolder.getAbsoluteFile.getName
    RunConfiguration(runConfigurationFile).map(
      runConfiguration =>
        RunfolderReadyForProcessing(RunId(runId),
                                    runFolder.getAbsolutePath,
                                    runConfiguration))
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

  def apply(content: String): Either[String, RunConfiguration] =
    scala.util
      .Try {

        val config = ConfigFactory.parseString(content)

        def getSelector(path: String) =
          if (config.hasPath(path)) Selector(config.getConfig(path))
          else Selector.empty

        def getDemultiplexings(config: Config) = DemultiplexingConfiguration(
          sampleSheet = config.getString("sampleSheet"),
          demultiplexingId = DemultiplexingId(config.getString("id")),
          readAssignment = {
            val list = config.getIntList("readAssignment").asScala
            (list(0), list(1))
          },
          umi =
            config.getIntList("umiReadNumber").asScala.headOption.map(_.toInt),
          extraBcl2FastqArguments =
            config.getStringList("extraBcl2FastqArguments").asScala
        )

        RunConfiguration(
          demultiplexingRuns = config
            .getConfigList("demultiplexing")
            .asScala
            .map(getDemultiplexings)
            .toSet,
          automatic = config.getBoolean("automatic"),
          referenceFasta = config.getString("referenceFasta"),
          targetIntervals = config.getString("targetIntervals"),
          bqsrKnownSites = config.getStringList("bqsr.knownSites").asScala.toSet,
          wesSelector = getSelector("wes"),
          rnaSelector = getSelector("rna"),
          geneModelGtf = config.getString("geneModelGtf"),
          globalIndexSet =
            if (config.hasPath("globalIndexSet"))
              Some(config.getString("globalIndexSet"))
            else None,
          dbSnpVcf = config.getString("dbSnpVcf"),
          variantEvaluationIntervals =
            config.getString("variantEvaluationIntervals"),
          vqsrMillsAnd1Kg = config.getString("vqsrMillsAnd1Kg"),
          vqsrHapmap = config.getString("vqsrHapmap"),
          vqsrOmni = config.getString("vqsrOmni"),
          vqsrOneKg = config.getString("vqsrOneKg")
        )
      }
      .toEither
      .left
      .map(_.toString)

  def apply(file: File): Either[String, RunConfiguration] =
    apply(fileutils.openSource(file)(_.mkString))

}
