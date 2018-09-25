package org.gc.pipelines.stages

import scala.concurrent.{ExecutionContext, Future}
import tasks._
import tasks.circesupport._
import org.gc.pipelines.application.{Pipeline, RunfolderReadyForProcessing}
import org.gc.pipelines.model._
import org.gc.pipelines.util.parseAsStringList
import java.io.File
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

class ProtoPipeline(implicit EC: ExecutionContext) extends Pipeline {
  def canProcess(r: RunfolderReadyForProcessing) = {
    val sampleSheet = r.sampleSheet.parsed
    sampleSheet.genomeCenterMetadata.contains("automatic") &&
    sampleSheet.genomeCenterMetadata.contains("referenceFasta") &&
    sampleSheet.runId.isDefined
  }

  def execute(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents) = {

    for {
      demultiplexed <- Demultiplexing.allLanes(r)(CPUMemoryRequest(1, 500))
      processedSamples <- ProtoPipeline.allSamples(PerSamplePipelineInput(
        r.sampleSheet,
        ProtoPipeline.groupBySample(demultiplexed.withoutUndetermined).toSet))(
        CPUMemoryRequest(1, 500))
    } yield true

  }

}

case class PerSamplePipelineInput(sampleSheet: SampleSheet,
                                  demultiplexed: Set[PerSampleFastQ])
    extends WithSharedFiles(demultiplexed.toSeq.flatMap(_.files): _*)

case class SingleSamplePipelineInput(sampleSheet: SampleSheet,
                                     demultiplexed: PerSampleFastQ,
                                     indexedReference: IndexedReferenceFasta)
    extends WithSharedFiles(demultiplexed.files ++ indexedReference.files: _*)

case class PerSamplePipelineResult(samples: Set[BamWithSampleMetadata])
    extends WithSharedFiles(samples.toSeq.flatMap(_.files): _*)

object ProtoPipeline {

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
    SharedFile(file, fileName).map(ReferenceFasta(_))
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
        for {
          vcf <- SharedFile(vcf, vcf.getName)
          vcfIdx <- SharedFile(vcfIdx, vcfIdx.getName)
        } yield VCF(vcf, Some(vcfIdx))
    }
    Future.sequence(vcfFilesFuture)
  }

  val singleSample =
    AsyncTask[SingleSamplePipelineInput, BamWithSampleMetadata](
      "__persample-single",
      1) {
      case SingleSamplePipelineInput(rawSampleSheet,
                                     demultiplexed,
                                     indexedReference) =>
        implicit computationEnvironment =>
          releaseResources
          val sampleSheet = rawSampleSheet.parsed
          computationEnvironment.withFilePrefix(
            Seq(demultiplexed.project, demultiplexed.runId)) {
            implicit computationEnvironment =>
              for {

                alignedSample <- BWAAlignment
                  .alignFastqPerSample(
                    PerSampleBWAAlignmentInput(demultiplexed.fastqs,
                                               demultiplexed.project,
                                               demultiplexed.sampleId,
                                               demultiplexed.runId,
                                               indexedReference))(
                    CPUMemoryRequest(1, 500))
                knownSites <- fetchKnownSitesFiles(sampleSheet)
                table <- BaseQualityScoreRecalibration.trainBQSR(
                  TrainBQSRInput(alignedSample.bam,
                                 indexedReference,
                                 knownSites.toSet))(CPUMemoryRequest(1, 1000))
                recalibrated <- BaseQualityScoreRecalibration.applyBQSR(
                  ApplyBQSRInput(alignedSample.bam, indexedReference, table))(
                  CPUMemoryRequest(1, 1000))
              } yield alignedSample.copy(bam = recalibrated)
          }

    }

  val allSamples =
    AsyncTask[PerSamplePipelineInput, PerSamplePipelineResult](
      "__persample-allsamples",
      1) {
      case PerSamplePipelineInput(rawSampleSheet, demultiplexed) =>
        implicit computationEnvironment =>
          releaseResources
          computationEnvironment.withFilePrefix(Seq("projects")) {
            implicit computationEnvironment =>
              val sampleSheet = rawSampleSheet.parsed

              for {
                referenceFasta <- fetchReference(sampleSheet)
                indexedFasta <- BWAAlignment.indexReference(referenceFasta)(
                  CPUMemoryRequest(1, 4000))
                processedSamples <- Future
                  .traverse(demultiplexed.toSeq) { perSampleFastQs =>
                    ProtoPipeline.singleSample(
                      SingleSamplePipelineInput(rawSampleSheet,
                                                perSampleFastQs,
                                                indexedFasta))(
                      CPUMemoryRequest(1, 500))
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
