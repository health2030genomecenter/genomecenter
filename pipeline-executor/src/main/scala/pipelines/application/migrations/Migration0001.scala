package org.gc.pipelines.application.migrations

import io.circe.Json
import org.gc.pipelines.application
import org.gc.pipelines.application.{DemultiplexingConfiguration}
import org.gc.pipelines.util.StableSet
import org.gc.pipelines.util.StableSet.syntax
import io.circe.syntax._

object Migration0001 extends Function1[Json, Json] {

  def apply(in: Json) = {
    val registered = in.hcursor
      .downField("Registered")
    if (registered.succeeded) {
      registered
        .downField("run")
        .downField("runConfiguration")
        .downField("rnaProcessing")
        .withFocus(_ => Json.arr())
        .up
        .withFocus(migrateRunConfiguration)
        .top
        .get
    } else in
  }

  private def migrateRunConfiguration(in: Json): Json = {
    val parsed =
      BeforeMigration.RunConfiguration.decoder.decodeJson(in).right.get

    val migrated: application.RunConfiguration = application.RunConfiguration(
      demultiplexingRuns = parsed.demultiplexingRuns,
      globalIndexSet = parsed.globalIndexSet,
      lastRunOfSamples = Set.empty.toStable
    )

    migrated.asJson

  }

  object BeforeMigration {

    case class RunConfiguration(
        demultiplexingRuns: StableSet[DemultiplexingConfiguration],
        globalIndexSet: Option[String]
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
