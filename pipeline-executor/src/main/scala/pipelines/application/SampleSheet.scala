package org.gc.pipelines.application

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class SampleSheet(content: String)

object SampleSheet {
  implicit val encoder: Encoder[SampleSheet] =
    deriveEncoder[SampleSheet]
  implicit val decoder: Decoder[SampleSheet] =
    deriveDecoder[SampleSheet]
}
