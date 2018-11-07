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
    wesBams: Set[SingleSamplePipelineResult],
    rnaSeqBams: Set[StarResult],
    fastp: Set[FastpReport]
) extends WithSharedFiles(
      fastqs.toSeq.flatMap(_.files) ++ wesBams.toSeq
        .flatMap(_.files) ++ rnaSeqBams.toSeq.flatMap(_.files) ++ fastp.toSeq
        .flatMap(_.files): _*)

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

  def extractBamAndVcfList(
      singleSampleResults: Set[SingleSamplePipelineResult]) =
    singleSampleResults.toSeq
      .flatMap { singleSampleResult =>
        List(
          (singleSampleResult.project, singleSampleResult.bam.bam),
          (singleSampleResult.project, singleSampleResult.gvcf.vcf),
          (singleSampleResult.project,
           singleSampleResult.haplotypeCallerReferenceCalls.vcf),
        )
      }
      .groupBy { case (project, _) => project }
      .map {
        case (project, pairs) =>
          (project, pairs.map(_._2))
      }

  def extractBamListFromRnaSeqResults(rnaSeqResults: Set[StarResult]) =
    rnaSeqResults.toSeq
      .map { case StarResult(_, bam) => (bam.project, bam.bam.file) }
      .groupBy { case (project, _) => project }
      .map {
        case (project, pairs) =>
          (project, pairs.map(_._2))
      }
  def extractFastp(fastpReports: Set[FastpReport]) =
    fastpReports.toSeq
      .map { fastpReport =>
        (fastpReport.project, fastpReport.html)
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
      case CollectDeliverablesInput(runId,
                                    fastqs,
                                    wesResults,
                                    rnaSeqResults,
                                    fastp) =>
        implicit computationEnvironment =>
          def inProjectFolder[T](project: Project) =
            appendToFilePrefix[T](Seq(project))

          val collectedFastqs: Map[Project, Seq[SharedFile]] =
            extractFastqList(fastqs)

          val wesBamAndVcfs: Map[Project, Seq[SharedFile]] =
            extractBamAndVcfList(wesResults)

          val collectedRnaSeqBam: Map[Project, Seq[SharedFile]] =
            extractBamListFromRnaSeqResults(rnaSeqResults)

          val collectedFastp: Map[Project, Seq[SharedFile]] =
            extractFastp(fastp)

          val collectedFiles = List(collectedRnaSeqBam,
                                    wesBamAndVcfs,
                                    collectedFastp,
                                    collectedFastqs)
            .reduce(
              tasks.util
                .addMaps(_, _)(_ ++ _))
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
