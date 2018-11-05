package org.gc.pipelines.model

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.generic.auto._

object FastpReport {

  case class Root(
      project: Project,
      sampleId: SampleId,
      runId: RunId,
      metrics: Metrics
  )

  case class Metrics(
      gcContent: Double,
      insertSizePeak: Int
  )

  case class InsertSizeObject(peak: Int)

  object Root {

    def apply(fastpJson: String,
              project: Project,
              sampleId: SampleId,
              runId: RunId): Root = {
      val jsonRoot = io.circe.parser.parse(fastpJson).right.get
      val insertSizePeak =
        jsonRoot.hcursor
          .downField("insert_size")
          .downField("peak")
          .as[Int]
          .right
          .get
      val gcContent = jsonRoot.hcursor
        .downField("summary")
        .downField("before_filtering")
        .downField("gc_content")
        .as[Double]
        .right
        .get
      Root(project, sampleId, runId, Metrics(gcContent, insertSizePeak))
    }

    implicit val encoder: Encoder[Root] =
      deriveEncoder[Root]
    implicit val decoder: Decoder[Root] =
      deriveDecoder[Root]

  }

}
