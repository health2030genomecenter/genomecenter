package org.gc.pipelines.application

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import com.typesafe.config.Config
import scala.collection.JavaConverters._
import org.gc.pipelines.model._
import org.gc.pipelines.util.StableSet
import org.gc.pipelines.util.StableSet.syntax

case class Selector(
    lanes: StableSet[Lane],
    samples: StableSet[SampleId],
    runIds: StableSet[RunId],
    projects: StableSet[Project]
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

  val empty =
    Selector(StableSet.empty, StableSet.empty, StableSet.empty, StableSet.empty)

  def apply(config: Config): Selector = {
    def getOrEmpty(path: String) =
      if (!config.hasPath(path)) StableSet.empty
      else config.getStringList(path).asScala.toSet.toStable
    Selector(
      lanes = getOrEmpty("lanes").map(_.toInt).map(Lane(_)),
      projects = getOrEmpty("projects").map(Project(_)),
      runIds = getOrEmpty("runIds").map(RunId(_)),
      samples = getOrEmpty("samples").map(SampleId(_)),
    )
  }
}
