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
    referenceFasta: String,
    targetIntervals: String,
    bqsrKnownSites: Set[String],
    wesSelector: Selector,
    rnaSelector: Selector,
    globalIndexSet: Option[String],
    geneModelGtf: String,
    dbSnpVcf: String,
    variantEvaluationIntervals: String,
    vqsrMillsAnd1Kg: Option[String],
    vqsrHapmap: Option[String],
    vqsrOneKgOmni: Option[String],
    vqsrOneKgHighConfidenceSnps: Option[String],
    vqsrDbSnp138: Option[String]
) {
  def toRunConfiguration = RunConfiguration(
    automatic = automatic,
    demultiplexingRuns = demultiplexingRuns.toStable,
    referenceFasta = referenceFasta,
    targetIntervals = targetIntervals,
    bqsrKnownSites = bqsrKnownSites.toStable,
    wesSelector = wesSelector,
    rnaSelector = rnaSelector,
    globalIndexSet = globalIndexSet,
    geneModelGtf = geneModelGtf,
    dbSnpVcf = dbSnpVcf,
    variantEvaluationIntervals = variantEvaluationIntervals,
    vqsrMillsAnd1Kg = vqsrMillsAnd1Kg,
    vqsrHapmap = vqsrHapmap,
    vqsrOneKgOmni = vqsrOneKgOmni,
    vqsrOneKgHighConfidenceSnps = vqsrOneKgHighConfidenceSnps,
    vqsrDbSnp138 = vqsrDbSnp138
  )
}

object RunConfigurationDTO {

  def apply(content: String): Either[String, RunConfigurationDTO] =
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
            config.getStringList("extraBcl2FastqArguments").asScala,
          tenX =
            if (config.hasPath("tenX")) Some(config.getBoolean("tenX"))
            else None
        )

        RunConfigurationDTO(
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
