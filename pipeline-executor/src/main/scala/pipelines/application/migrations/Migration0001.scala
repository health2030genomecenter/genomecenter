package org.gc.pipelines.application.migrations

import io.circe.Json
import org.gc.pipelines.util.StableSet._
import org.gc.pipelines.application.{Selector, RNASeqConfiguration}
import org.gc.pipelines.model.AnalysisId
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
        .withFocus(migrate)
        .top
        .get
    } else in
  }

  private def migrate(in: Json): Json = {
    val parsed: Seq[(Selector, BeforeMigration.RNASeqConfiguration)] =
      BeforeMigration.decoder.decodeJson(in).right.get

    val migrated = parsed
      .map {
        case (selector, parsed) =>
          (selector,
           RNASeqConfiguration(parsed.analysisId,
                               parsed.referenceFasta,
                               geneModelGtf = parsed.geneModelGtf,
                               Nil,
                               quantificationGtf = parsed.geneModelGtf))
      }
      .toSet
      .toStable

    migrated.asJson
  }

  object BeforeMigration {

    val decoder = io.circe.Decoder.apply[Seq[(Selector, RNASeqConfiguration)]]

    case class RNASeqConfiguration(
        analysisId: AnalysisId,
        referenceFasta: String,
        geneModelGtf: String
    )

    object RNASeqConfiguration {
      import io.circe.generic.semiauto._
      import io.circe._
      implicit val encoder: Encoder[RNASeqConfiguration] =
        deriveEncoder[RNASeqConfiguration]
      implicit val decoder: Decoder[RNASeqConfiguration] =
        deriveDecoder[RNASeqConfiguration]

    }

  }

}
