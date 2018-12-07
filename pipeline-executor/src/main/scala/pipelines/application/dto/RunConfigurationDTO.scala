package org.gc.pipelines.application.dto

import java.io.File
import com.typesafe.config.{ConfigFactory, Config}
import scala.collection.JavaConverters._

import org.gc.pipelines.application._
import org.gc.pipelines.model._
import org.gc.pipelines.util.StableSet.syntax

case class RunConfigurationDTO(
    automatic: Boolean,
    demultiplexingRuns: Set[DemultiplexingConfiguration],
    globalIndexSet: Option[String],
    wesProcessing: List[(Selector, WESConfiguration)],
    rnaProcessing: List[(Selector, RNASeqConfiguration)]
) {
  def toRunConfiguration = RunConfiguration(
    automatic = automatic,
    demultiplexingRuns = demultiplexingRuns.toStable,
    globalIndexSet = globalIndexSet,
    wesProcessing = wesProcessing.toSet.toStable,
    rnaProcessing = rnaProcessing.toSet.toStable
  )
}

object RunConfigurationDTO {

  def apply(content: String): Either[String, RunConfigurationDTO] =
    scala.util
      .Try {

        val config = ConfigFactory.parseString(content)

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
            config.getStringList("extraBcl2FastqArguments").asScala,
          tenX =
            if (config.hasPath("tenX")) Some(config.getBoolean("tenX"))
            else None
        )

        def parseRNASeqConfiguration(config: Config) = RNASeqConfiguration(
          referenceFasta = config.getString("referenceFasta"),
          geneModelGtf = config.getString("geneModelGtf")
        )
        def parseWESConfiguration(config: Config) = WESConfiguration(
          referenceFasta = config.getString("referenceFasta"),
          targetIntervals = config.getString("targetIntervals"),
          bqsrKnownSites =
            config.getStringList("bqsr.knownSites").asScala.toSet.toStable,
          dbSnpVcf = config.getString("dbSnpVcf"),
          variantEvaluationIntervals =
            config.getString("variantEvaluationIntervals"),
          vqsrMillsAnd1Kg =
            option(config, "vqsrMillsAnd1Kg")(c => p => c.getString(p)),
          vqsrHapmap = option(config, "vqsrHapmap")(c => p => c.getString(p)),
          vqsrOneKgOmni =
            option(config, "vqsrOneKgOmni")(c => p => c.getString(p)),
          vqsrOneKgHighConfidenceSnps =
            option(config, "vqsrOneKgHighConfidenceSnps")(c =>
              p => c.getString(p)),
          vqsrDbSnp138 =
            option(config, "vqsrDbSnp138")(c => p => c.getString(p))
        )

        RunConfigurationDTO(
          demultiplexingRuns = config
            .getConfigList("demultiplexing")
            .asScala
            .map(getDemultiplexings)
            .toSet,
          automatic = config.getBoolean("automatic"),
          globalIndexSet =
            if (config.hasPath("globalIndexSet"))
              Some(config.getString("globalIndexSet"))
            else None,
          wesProcessing =
            if (config.hasPath("wes"))
              config.getConfigList("wes").asScala.toList.map { config =>
                (Selector(config), parseWESConfiguration(config))
              } else Nil,
          rnaProcessing =
            if (config.hasPath("rna"))
              config.getConfigList("rna").asScala.toList.map { config =>
                (Selector(config), parseRNASeqConfiguration(config))
              } else Nil
        )

      }
      .toEither
      .left
      .map(_.toString)

  private def option[T](config: Config, path: String)(
      extract: Config => String => T): Option[T] =
    if (config.hasPath(path)) Some(extract(config)(path))
    else None

  def apply(file: File): Either[String, RunConfigurationDTO] =
    apply(fileutils.openSource(file)(_.mkString))
}
