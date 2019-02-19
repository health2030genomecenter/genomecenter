package org.gc.pipelines.application.migrations

import io.circe.Json
import org.gc.pipelines.util.StableSet
import org.gc.pipelines.application.{DemultiplexingConfiguration}
import org.gc.pipelines.application
import io.circe.syntax._

object Migration0000 extends Function1[Json, Json] {

  def apply(in: Json) = {
    val registered = in.hcursor
      .downField("Registered")
    if (registered.succeeded) {
      registered
        .downField("run")
        .downField("runConfiguration")
        .withFocus(migrate)
        .top
        .get
    } else in
  }

  private def migrate(in: Json): Json = {
    val parsed =
      BeforeMigration.RunConfiguration.decoder.decodeJson(in).right.get

    val migrated: application.RunConfiguration = application.RunConfiguration(
      demultiplexingRuns = parsed.demultiplexingRuns,
      globalIndexSet = parsed.globalIndexSet
    )

    migrated.asJson
  }

  object BeforeMigration {

    case class RunConfiguration(
        demultiplexingRuns: StableSet[DemultiplexingConfiguration],
        referenceFasta: String,
        targetIntervals: String,
        bqsrKnownSites: StableSet[String],
        globalIndexSet: Option[String],
        geneModelGtf: String,
        dbSnpVcf: String,
        variantEvaluationIntervals: String,
        vqsrMillsAnd1Kg: Option[String],
        vqsrHapmap: Option[String],
        vqsrOneKgOmni: Option[String],
        vqsrOneKgHighConfidenceSnps: Option[String],
        vqsrDbSnp138: Option[String]
    )

    object RunConfiguration {
      import io.circe.generic.semiauto._
      import io.circe._
      implicit val encoder: Encoder[RunConfiguration] =
        deriveEncoder[RunConfiguration]
      implicit val decoder: Decoder[RunConfiguration] =
        deriveDecoder[RunConfiguration]

    }

  }

}
