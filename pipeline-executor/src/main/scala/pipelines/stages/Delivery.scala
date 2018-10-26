package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.gc.pipelines.model._
import scala.concurrent.Future
import akka.stream.scaladsl.Source
import akka.util.ByteString

case class CollectDeliverablesInput(
    runId: RunId,
    fastqs: Set[PerSampleFastQ],
    bams: Set[SingleSamplePipelineResult]
) extends WithSharedFiles(
      fastqs.toSeq.flatMap(_.files) ++ bams.toSeq.flatMap(_.files): _*)

case class DeliverableList(lists: Seq[(Project, SharedFile)])
    extends WithSharedFiles(lists.map(_._2): _*)

object Delivery {

  def extractFastqList(
      fastqs: Set[PerSampleFastQ]): Map[Project, Seq[SharedFile]] =
    fastqs.toSeq
      .map {
        case PerSampleFastQ(lanes, project, _, _) =>
          val fastqs = lanes.toSeq
            .flatMap(lane => List(lane.read1, lane.read2) ++ lane.umi.toList)
            .map(_.file)
          (project, fastqs)
      }
      .groupBy { case (project, _) => project }
      .map { case (key, value) => (key, value.map(_._2).flatten) }

  def extractBamList(singleSampleResults: Set[SingleSamplePipelineResult]) =
    singleSampleResults.toSeq
      .map { singleSampleResult =>
        (singleSampleResult.project, singleSampleResult.bam.bam)
      }
      .groupBy { case (project, _) => project }
      .map {
        case (project, pairs) =>
          (project, pairs.map(_._2))
      }

  val collectDeliverables =
    AsyncTask[CollectDeliverablesInput, DeliverableList](
      "__collectdeliverables",
      1) {
      case CollectDeliverablesInput(runId, fastqs, singleSampleResults) =>
        implicit computationEnvironment =>
          def inProjectFolder[T](project: Project) =
            appendToFilePrefix[T](Seq(project))

          val collectedFastqs: Map[Project, Seq[SharedFile]] =
            extractFastqList(fastqs)

          val collectedCoordinateSortedBams: Map[Project, Seq[SharedFile]] =
            extractBamList(singleSampleResults)

          val collectedFiles = tasks.util
            .addMaps(collectedFastqs, collectedCoordinateSortedBams)(_ ++ _)
            .toSeq

          for {
            pathLists <- Future.traverse(collectedFiles) {
              case (project, files) =>
                for {
                  pathList <- Future.traverse(files)(_.uri.map(_.path))
                } yield (project, pathList)
            }
            fileList <- Future.traverse(pathLists) {
              case (project, pathList) =>
                val source =
                  Source.single(ByteString(pathList.sorted.mkString("\n")))

                for {
                  pathListFile <- inProjectFolder(project) {
                    implicit computationEnvironment =>
                      SharedFile(source,
                                 project + "." + runId + ".deliverables.list")
                  }
                } yield (project, pathListFile)
            }
          } yield DeliverableList(fileList)

    }

}

object CollectDeliverablesInput {
  implicit val encoder: Encoder[CollectDeliverablesInput] =
    deriveEncoder[CollectDeliverablesInput]
  implicit val decoder: Decoder[CollectDeliverablesInput] =
    deriveDecoder[CollectDeliverablesInput]
}

object DeliverableList {
  implicit val encoder: Encoder[DeliverableList] =
    deriveEncoder[DeliverableList]
  implicit val decoder: Decoder[DeliverableList] =
    deriveDecoder[DeliverableList]
}
