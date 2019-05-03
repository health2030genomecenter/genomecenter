package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.gc.pipelines.model._
import scala.concurrent.Future
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.gc.pipelines.util.StableSet
import org.gc.pipelines.util.traverseAll
import org.gc.pipelines.application.ProgressData
import ProgressData.DeliveryListAvailable
import org.gc.pipelines.util.ResourceConfig

case class CollectDeliverablesInput(
    samples: StableSet[SampleResult],
    other: StableSet[(Project, SharedFile)]
) extends WithSharedFiles(
      samples.toSeq.flatMap(_.files) ++ other.toSeq.map(_._2): _*)

case class DeliverableList(lists: Seq[(Project, SharedFile, ProgressData)])
    extends WithSharedFiles(lists.map(_._2): _*)

object Delivery {

  def md5 = AsyncTask[SharedFile, String]("__md5", 1) {
    file => implicit computationEnvironment =>
      implicit val mat = computationEnvironment.components.actorMaterializer

      val digest = java.security.MessageDigest.getInstance("MD5")

      for {
        _ <- file.source.runForeach { byteString =>
          digest.update(byteString.toArray)
        }
      } yield
        javax.xml.bind.DatatypeConverter
          .printHexBinary(digest.digest())
          .toLowerCase()

  }

  def extractFastqList(
      fastqs: Set[PerSamplePerRunFastQ]): Map[Project, Seq[SharedFile]] =
    fastqs.toSeq
      .map {
        case PerSamplePerRunFastQ(lanes, project, _, _) =>
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
      .flatMap(_.mergedRuns)
      .flatMap { singleSampleResult =>
        List(
          (singleSampleResult.project, singleSampleResult.bam.bam),
          (singleSampleResult.project, singleSampleResult.referenceFasta.fasta)
        ) ++
          singleSampleResult.gvcf.toSeq
            .map(vcf => (singleSampleResult.project, vcf.vcf))
            .toList ++
          singleSampleResult.haplotypeCallerReferenceCalls.toSeq
            .map(vcf => (singleSampleResult.project, vcf.vcf))
            .toList
      }
      .groupBy { case (project, _) => project }
      .map {
        case (project, pairs) =>
          (project, pairs.map(_._2).distinct)
      }

  def extractBamListFromRnaSeqResults(rnaSeqResults: Set[StarResult]) =
    rnaSeqResults.toSeq
      .map { case StarResult(_, _, bam) => (bam.project, bam.bam.file) }
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

  def inAll(runs: Seq[SampleResult])(
      f: SampleResult => Map[Project, Seq[SharedFile]]) =
    runs
      .map(f)
      .reduce((a, b) => tasks.util.addMaps(a, b)(_ ++ _))

  val collectDeliverables =
    AsyncTask[CollectDeliverablesInput, DeliverableList](
      "__collectdeliverables",
      1) {
      case CollectDeliverablesInput(samples, otherFiles) =>
        implicit computationEnvironment =>
          def inProjectFolder[T](project: Project) =
            appendToFilePrefix[T](Seq(project))

          val collectedFastqs: Map[Project, Seq[SharedFile]] =
            inAll(samples.toSeq)(sample =>
              extractFastqList(sample.demultiplexed.toSet))

          val wesBamAndVcfs: Map[Project, Seq[SharedFile]] =
            inAll(samples.toSeq)(sample =>
              extractBamAndVcfList(sample.wes.map(_._1).toSet))

          val collectedRnaSeqBam: Map[Project, Seq[SharedFile]] =
            inAll(samples.toSeq)(sample =>
              extractBamListFromRnaSeqResults(sample.rna.map(_.star).toSet))

          val collectedRnaSeqQuantification: Map[Project, Seq[SharedFile]] =
            inAll(samples.toSeq)(sample =>
              Map(sample.project -> sample.rna.flatMap(_.quantification.files)))

          val collectedFastp: Map[Project, Seq[SharedFile]] =
            inAll(samples.toSeq)(sample =>
              extractFastp(sample.fastpReports.toSet))

          val collectedOtherFiles =
            otherFiles.toSeq.groupBy(_._1).map(x => x._1 -> x._2.map(_._2))

          val collectedFilesWithProject: Seq[(Project, SharedFile)] =
            List(collectedRnaSeqBam,
                 wesBamAndVcfs,
                 collectedFastp,
                 collectedFastqs,
                 collectedOtherFiles,
                 collectedRnaSeqQuantification)
              .reduce(tasks.util
                .addMaps(_, _)(_ ++ _))
              .toSeq
              .flatMap {
                case (project, files) => files.map(f => (project, f))
              }

          def createMd5Summary(
              filesWithMd5: Seq[(SharedFile, String)]): String = {
            filesWithMd5
              .map {
                case (file, md5) =>
                  s"MD5(${file.path.toString}) = $md5"
              }
              .mkString("\n")
          }

          releaseResources

          for {
            withMd5Flattened <- traverseAll(collectedFilesWithProject) {
              case (project, file) =>
                md5(file)(ResourceConfig.minimal).map { md5 =>
                  (project, file, md5)
                }
            }
            withMd5PerProject = withMd5Flattened
              .groupBy { case (project, _, _) => project }
              .map {
                case (project, group) =>
                  (project, group.map { case (_, file, md5) => (file, md5) })
              }
            pathLists <- Future.traverse(withMd5PerProject.toSeq) {
              case (project, files) =>
                for {
                  md5File <- inProjectFolder(project) {
                    implicit computationEnvironment =>
                      SharedFile(
                        Source.single(ByteString(createMd5Summary(files))),
                        "md5.txt")
                  }
                  pathList <- Future.traverse(md5File +: files.map(_._1))(
                    _.uri.map(_.path))

                } yield {

                  (project, pathList.distinct)
                }
            }
            fileList <- Future.traverse(pathLists) {
              case (project, pathList) =>
                val runsIncluded = samples
                  .flatMap(_.runFolders)
                  .toSeq
                  .map(_.runId)
                  .distinct
                  .sortBy(_.toString)

                val deliveryAvailable = DeliveryListAvailable(
                  project,
                  samples.map(_.sampleId).toSeq.sortBy(_.toString),
                  pathList.sorted,
                  runsIncluded.sortBy(_.toString)
                )

                val source =
                  Source.single(
                    ByteString("#runs included: " + runsIncluded
                      .mkString(".") + "\n\n" + pathList.sorted.mkString("\n")))

                for {
                  pathListFile <- inProjectFolder(project) {
                    implicit computationEnvironment =>
                      SharedFile(
                        source,
                        project + s".r${runsIncluded.size}-h${runsIncluded.hashCode}.s" + samples.size + ".deliverables.list")
                  }
                } yield (project, pathListFile, deliveryAvailable)
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
