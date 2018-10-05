package org.gc.pipelines.model

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.generic.auto._

object FastpReport {

  case class Root(
      project: Project,
      sampleId: SampleId,
      runId: RunId,
      lane: Lane,
      metrics: Metrics
  )

  case class Metrics(
      fake: Boolean
  )

  object Root {

    def apply(fastpJson: String,
              project: Project,
              sampleId: SampleId,
              runId: RunId,
              lane: Lane): Root = {
      val _ = fastpJson
      Root(project, sampleId, runId, lane, Metrics(true))
    }

    implicit val encoder: Encoder[Root] =
      deriveEncoder[Root]
    implicit val decoder: Decoder[Root] =
      deriveDecoder[Root]

  }

}
