package org.gc.pipelines.util

import shapeless.tag.@@
import shapeless.tag
import io.circe.{Encoder, Decoder}

object Circe {
  def stringCodec[Tag]: (Encoder[String @@ Tag], Decoder[String @@ Tag]) = {
    val enc: Encoder[String @@ Tag] = Encoder.encodeString.contramap(identity)
    val dec: Decoder[String @@ Tag] =
      Decoder.decodeString.map(tag[Tag][String](_))
    (enc, dec)
  }
}
