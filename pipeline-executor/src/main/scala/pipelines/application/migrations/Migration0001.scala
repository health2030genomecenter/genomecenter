package org.gc.pipelines.application.migrations

import io.circe.Json

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
        .top
        .get
    } else in
  }

}
