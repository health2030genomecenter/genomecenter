package org.gc.pipelines.stages

import scala.concurrent.{ExecutionContext, Future}
import tasks._
import tasks.circesupport._
import org.gc.pipelines.application.{Pipeline, RunfolderReadyForProcessing}
import org.gc.pipelines.model._
import org.gc.pipelines.util.{parseAsStringList, ResourceConfig}
import java.io.File
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import com.typesafe.scalalogging.StrictLogging
import scala.util.{Success, Failure}

class ProtoPipeline(implicit EC: ExecutionContext)
    extends Pipeline
    with StrictLogging {
  def canProcess(r: RunfolderReadyForProcessing) = {
    val sampleSheet = r.sampleSheet.parsed
    sampleSheet.genomeCenterMetadata.contains("automatic") &&
    sampleSheet.genomeCenterMetadata.contains("referenceFasta") &&
    sampleSheet.runId.isDefined
  }

  def execute(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents) = {

    val sampleSheet = r.sampleSheet.parsed

    logger.debug(s"${r.runId} Parsed sample sheet as $sampleSheet")

    for {
      reference <- ProtoPipeline.fetchReference(sampleSheet)
      knownSites <- ProtoPipeline.fetchKnownSitesFiles(sampleSheet)

      demultiplexed <- Demultiplexing.allLanes(r)(ResourceConfig.minimal)
      processedSamples <- ProtoPipeline.allSamples(
        PerSamplePipelineInput(
          ProtoPipeline
            .groupBySample(demultiplexed.withoutUndetermined)
            .toSet,
          reference,
          knownSites.toSet
        ))(ResourceConfig.minimal)
    } yield true

  }

}

case class PerSamplePipelineInput(demultiplexed: Set[PerSampleFastQ],
                                  reference: ReferenceFasta,
                                  knownSites: Set[VCF])
    extends WithSharedFiles(
      demultiplexed.toSeq.flatMap(_.files) ++ reference.files ++ knownSites
        .flatMap(_.files): _*)

case class SingleSamplePipelineInput(demultiplexed: PerSampleFastQ,
                                     knownSites: Set[VCF],
                                     indexedReference: IndexedReferenceFasta)
    extends WithSharedFiles(
      demultiplexed.files ++ indexedReference.files ++ knownSites.flatMap(
        _.files): _*)

case class PerSamplePipelineResult(samples: Set[BamWithSampleMetadata])
    extends WithSharedFiles(samples.toSeq.flatMap(_.files): _*)

object ProtoPipeline extends StrictLogging {

  private def selectReadType(fqs: Seq[FastQWithSampleMetadata],
                             readType: ReadType) =
    fqs
      .filter(_.readType == readType)
      .headOption
      .map(_.fastq)

  def groupBySample(demultiplexed: DemultiplexedReadData): Seq[PerSampleFastQ] =
    demultiplexed.fastqs
      .groupBy { fq =>
        (fq.project, fq.sampleId, fq.runId)
      }
      .toSeq
      .map {
        case ((project, sampleId, runId), perSampleFastQs) =>
          val perLaneFastQs =
            perSampleFastQs
              .groupBy(_.lane)
              .toSeq
              .map(_._2)
              .map { (fqsInLane: Set[FastQWithSampleMetadata]) =>
                val maybeRead1 =
                  selectReadType(fqsInLane.toSeq, ReadType("R1"))
                val maybeRead2 =
                  selectReadType(fqsInLane.toSeq, ReadType("R2"))

                val lane = {
                  val distinctLanesInGroup = fqsInLane.map(_.lane)
                  assert(distinctLanesInGroup.size == 1) // due to groupBy
                  distinctLanesInGroup.head
                }

                for {
                  read1 <- maybeRead1
                  read2 <- maybeRead2
                } yield FastQPerLane(lane, read1, read2)
              }
              .flatten
          PerSampleFastQ(
            perLaneFastQs.toSet,
            project,
            sampleId,
            runId
          )
      }

  private def fetchReference(sampleSheet: SampleSheet.ParsedData)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) = {
    val file = new File(sampleSheet.genomeCenterMetadata("referenceFasta"))
    val fileName = file.getName
    logger.debug(s"Fetching reference $file")
    SharedFile(file, fileName).map(ReferenceFasta(_)).andThen {
      case Success(_) =>
        logger.debug(s"Fetched reference")
      case Failure(e) =>
        logger.error(s"Failed to fetch reference $file", e)

    }
  }

  private def fetchKnownSitesFiles(sampleSheet: SampleSheet.ParsedData)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) = {
    val files = parseAsStringList(
      sampleSheet.genomeCenterMetadata("bqsr.knownSites")).right.get

    val fileListWithIndices = files.map { vcfFile =>
      (new File(vcfFile), new File(vcfFile + ".idx"))
    }
    val vcfFilesFuture = fileListWithIndices.map {
      case (vcf, vcfIdx) =>
        logger.debug(s"Fetching known sites vcf $vcf with its index $vcfIdx")
        for {
          vcf <- SharedFile(vcf, vcf.getName)
          vcfIdx <- SharedFile(vcfIdx, vcfIdx.getName)
        } yield VCF(vcf, Some(vcfIdx))
    }
    Future.sequence(vcfFilesFuture).andThen {
      case Success(_) =>
        logger.debug(s"Fetched known sites vcfs")
      case Failure(e) =>
        logger.error("Failed to fetch known sites files", e)
    }
  }

  val singleSample =
    AsyncTask[SingleSamplePipelineInput, BamWithSampleMetadata](
      "__persample-single",
      1) {
      case SingleSamplePipelineInput(demultiplexed,
                                     knownSites,
                                     indexedReference) =>
        implicit computationEnvironment =>
          log.info(s"Processing demultiplexed sample $demultiplexed")
          releaseResources

          def intoIntermediateFolder[T] =
            appendToFilePrefix[T](
              Seq(demultiplexed.project, demultiplexed.runId, "intermediate"))

          def intoFinalFolder[T] =
            appendToFilePrefix[T](
              Seq(demultiplexed.project, demultiplexed.runId))

          for {

            alignedSample <- intoIntermediateFolder {
              implicit computationEnvironment =>
                BWAAlignment
                  .alignFastqPerSample(
                    PerSampleBWAAlignmentInput(demultiplexed.fastqs,
                                               demultiplexed.project,
                                               demultiplexed.sampleId,
                                               demultiplexed.runId,
                                               indexedReference))(
                    ResourceConfig.minimal)
            }
            table <- intoIntermediateFolder { implicit computationEnvironment =>
              BaseQualityScoreRecalibration.trainBQSR(
                TrainBQSRInput(alignedSample.bam,
                               indexedReference,
                               knownSites.toSet))(ResourceConfig.trainBqsr)
            }
            recalibrated <- intoFinalFolder { implicit computationEnvironment =>
              BaseQualityScoreRecalibration.applyBQSR(
                ApplyBQSRInput(alignedSample.bam, indexedReference, table))(
                ResourceConfig.applyBqsr)
            }
          } yield alignedSample.copy(bam = recalibrated)

    }

  val allSamples =
    AsyncTask[PerSamplePipelineInput, PerSamplePipelineResult](
      "__persample-allsamples",
      1) {
      case PerSamplePipelineInput(demultiplexed, referenceFasta, knownSites) =>
        implicit computationEnvironment =>
          releaseResources
          computationEnvironment.withFilePrefix(Seq("projects")) {
            implicit computationEnvironment =>
              for {
                indexedFasta <- BWAAlignment.indexReference(referenceFasta)(
                  ResourceConfig.indexReference)
                processedSamples <- Future
                  .traverse(demultiplexed.toSeq) { perSampleFastQs =>
                    ProtoPipeline.singleSample(
                      SingleSamplePipelineInput(perSampleFastQs,
                                                knownSites,
                                                indexedFasta))(
                      ResourceConfig.minimal)
                  }
              } yield PerSamplePipelineResult(processedSamples.toSet)
          }

    }

}

object SingleSamplePipelineInput {
  implicit val encoder: Encoder[SingleSamplePipelineInput] =
    deriveEncoder[SingleSamplePipelineInput]
  implicit val decoder: Decoder[SingleSamplePipelineInput] =
    deriveDecoder[SingleSamplePipelineInput]
}

object PerSamplePipelineInput {
  implicit val encoder: Encoder[PerSamplePipelineInput] =
    deriveEncoder[PerSamplePipelineInput]
  implicit val decoder: Decoder[PerSamplePipelineInput] =
    deriveDecoder[PerSamplePipelineInput]
}

object PerSamplePipelineResult {
  implicit val encoder: Encoder[PerSamplePipelineResult] =
    deriveEncoder[PerSamplePipelineResult]
  implicit val decoder: Decoder[PerSamplePipelineResult] =
    deriveDecoder[PerSamplePipelineResult]
}
