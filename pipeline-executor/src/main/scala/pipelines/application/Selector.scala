package org.gc.pipelines.application

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import com.typesafe.config.Config
import scala.collection.JavaConverters._
import org.gc.pipelines.model._

case class Selector(
    lanes: Set[Lane],
    samples: Set[SampleId],
    runIds: Set[RunId],
    projects: Set[Project]
) {
  def isSelected(sample: Metadata): Boolean =
    lanes.contains(sample.lane) ||
      samples.contains(sample.sample) ||
      runIds.contains(sample.runId) ||
      projects.contains(sample.project)
}

object Selector {
  implicit val encoder: Encoder[Selector] =
    deriveEncoder[Selector]
  implicit val decoder: Decoder[Selector] =
    deriveDecoder[Selector]

  val empty = Selector(Set.empty, Set.empty, Set.empty, Set.empty)

  def apply(config: Config): Selector = {
    def getOrEmpty(path: String) =
      if (!config.hasPath(path)) Set.empty
      else config.getStringList(path).asScala.toSet
    Selector(
      lanes = getOrEmpty("lanes").map(_.toInt).map(Lane(_)),
      projects = getOrEmpty("projects").map(Project(_)),
      runIds = getOrEmpty("runIds").map(RunId(_)),
      samples = getOrEmpty("samples").map(SampleId(_)),
    )
  }
}
