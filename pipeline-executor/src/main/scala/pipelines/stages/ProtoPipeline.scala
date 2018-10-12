package org.gc.pipelines.stages

import scala.concurrent.{ExecutionContext, Future}
import tasks._
import tasks.circesupport._
import org.gc.pipelines.application.{
  Pipeline,
  RunfolderReadyForProcessing,
  RunConfiguration
}
import org.gc.pipelines.model._
import org.gc.pipelines.util.ResourceConfig
import java.io.File
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import com.typesafe.scalalogging.StrictLogging
import scala.util.{Success, Failure}

class ProtoPipeline(implicit EC: ExecutionContext)
    extends Pipeline
    with StrictLogging {
  def canProcess(r: RunfolderReadyForProcessing) = {
    r.runConfiguration.automatic
  }

  def execute(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents) = {

    def inRunQCFolder[T](f: TaskSystemComponents => T) =
      tsc.withFilePrefix(Seq("runQC"))(f)

    for {
      reference <- ProtoPipeline.fetchReference(r.runConfiguration)
      knownSites <- ProtoPipeline.fetchKnownSitesFiles(r.runConfiguration)
      sampleSheet <- ProtoPipeline.fetchSampleSheet(r.runConfiguration)
      selectionTargetIntervals <- ProtoPipeline.fetchTargetIntervals(
        r.runConfiguration)

      demultiplexed <- Demultiplexing.allLanes(
        DemultiplexingInput(RunId(r.runId),
                            r.runFolderPath,
                            sampleSheet,
                            r.runConfiguration.extraBcl2FastqArguments))(
        ResourceConfig.minimal)

      perSampleFastQs = ProtoPipeline
        .groupBySample(demultiplexed.withoutUndetermined,
                       r.runConfiguration.readAssignment,
                       r.runConfiguration.umi)

      fastpReports = startFastpReports(perSampleFastQs)

      perSampleResults <- ProtoPipeline.allSamples(
        PerSamplePipelineInput(
          perSampleFastQs.toSet,
          reference,
          knownSites.toSet,
          selectionTargetIntervals
        ))(ResourceConfig.minimal)

      fastpReports <- fastpReports

      sampleQCs = extractQCFiles(perSampleResults, fastpReports)

      _ <- inRunQCFolder { implicit tsc =>
        AlignmentQC.runQCTable(RunQCTableInput(RunId(r.runId), sampleQCs))(
          ResourceConfig.minimal)
      }

    } yield true

  }

  def extractQCFiles(sampleResults: PerSamplePipelineResult,
                     fastpReports: Seq[FastpReport]): Seq[SampleMetrics] =
    sampleResults.samples.toSeq.map { sample =>
      val fastpReportsOfSample = fastpReports.filter { fp =>
        fp.sampleId == sample.sampleId &&
        fp.project == sample.project &&
        fp.runId == sample.runId
      }
      SampleMetrics(
        sample.alignmentQC.alignmentSummary,
        sample.targetSelectionQC.hsMetrics,
        sample.duplicationQC.markDuplicateMetrics,
        fastpReportsOfSample,
        sample.project,
        sample.sampleId,
        sample.runId
      )
    }

  def startFastpReports(perSampleFastQs: Seq[PerSampleFastQ])(
      implicit tsc: TaskSystemComponents): Future[Seq[FastpReport]] = {
    val fastqsPerLanePerSample = for {
      sample <- perSampleFastQs
      lane <- sample.lanes
    } yield
      FastQPerLaneWithMetadata(lane,
                               sample.project,
                               sample.sampleId,
                               sample.runId)

    Future.traverse(fastqsPerLanePerSample)(fq =>
      tsc.withFilePrefix(Seq("demultiplex", fq.runId, fq.lane.lane.toString)) {
        implicit tsc =>
          Fastp.report(fq)(ResourceConfig.fastp)
    })
  }

}

case class PerSamplePipelineInput(demultiplexed: Set[PerSampleFastQ],
                                  reference: ReferenceFasta,
                                  knownSites: Set[VCF],
                                  selectionTargetIntervals: BedFile)
    extends WithSharedFiles(
      demultiplexed.toSeq.flatMap(_.files) ++ reference.files ++ knownSites
        .flatMap(_.files) ++ selectionTargetIntervals.files: _*)

case class SingleSamplePipelineInput(demultiplexed: PerSampleFastQ,
                                     knownSites: Set[VCF],
                                     indexedReference: IndexedReferenceFasta,
                                     selectionTargetIntervals: BedFile)
    extends WithSharedFiles(
      demultiplexed.files ++ indexedReference.files ++ knownSites.flatMap(
        _.files) ++ selectionTargetIntervals.files: _*)

case class SingleSamplePipelineResult(bam: CoordinateSortedBam,
                                      project: Project,
                                      sampleId: SampleId,
                                      runId: RunId,
                                      alignmentQC: AlignmentQCResult,
                                      duplicationQC: DuplicationQCResult,
                                      targetSelectionQC: SelectionQCResult)
    extends WithSharedFiles(
      bam.files ++ alignmentQC.files ++ duplicationQC.files ++ targetSelectionQC.files: _*)

case class PerSamplePipelineResult(samples: Set[SingleSamplePipelineResult])
    extends WithSharedFiles(samples.toSeq.flatMap(_.files): _*)

object ProtoPipeline extends StrictLogging {

  private def selectReadType(fqs: Seq[FastQWithSampleMetadata],
                             readType: ReadType) =
    fqs
      .filter(_.readType == readType)
      .headOption
      .map(_.fastq)

  /**
    * @param readAssignment Mapping between members of a read pair and numbers assigned by bcl2fastq.
    * @param umi Number assigned by bcl2fastq, if any
    *
    * e.g. if R1 is the first member of the pair and R2 is the second member of the pair
    * then (1,2)
    * if R1 is the first member of the pair, R2 is the UMI, R3 is the second member of the pair
    * then (1,3) and if you want to process the umi then pass Some(2) to the umi param.
    * if R1 is the second member of the pair (for whatever reason) and R2 is the first then pass (2,1)
    */
  def groupBySample(demultiplexed: DemultiplexedReadData,
                    readAssignment: (Int, Int),
                    umi: Option[Int]): Seq[PerSampleFastQ] =
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
                  selectReadType(fqsInLane.toSeq,
                                 ReadType("R" + readAssignment._1))

                val maybeRead2 =
                  selectReadType(fqsInLane.toSeq,
                                 ReadType("R" + readAssignment._2))

                val maybeUmi = umi.flatMap(umiReadNumber =>
                  selectReadType(fqsInLane.toSeq,
                                 ReadType("R" + umiReadNumber)))

                val lane = {
                  val distinctLanesInGroup = fqsInLane.map(_.lane)
                  assert(distinctLanesInGroup.size == 1) // due to groupBy
                  distinctLanesInGroup.head
                }

                for {
                  read1 <- maybeRead1
                  read2 <- maybeRead2
                } yield FastQPerLane(lane, read1, read2, maybeUmi)
              }
              .flatten
          PerSampleFastQ(
            perLaneFastQs.toSet,
            project,
            sampleId,
            runId
          )
      }

  private def fetchReference(runConfiguration: RunConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) = {
    tsc.withFilePrefix(Seq("references")) { implicit tsc =>
      val file = new File(runConfiguration.referenceFasta)
      val fileName = file.getName
      logger.debug(s"Fetching reference $file")
      SharedFile(file, fileName).map(ReferenceFasta(_)).andThen {
        case Success(_) =>
          logger.debug(s"Fetched reference")
        case Failure(e) =>
          logger.error(s"Failed to fetch reference $file", e)

      }
    }
  }

  private def fetchSampleSheet(runConfiguration: RunConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) = {
    tsc.withFilePrefix(Seq("sampleSheets")) { implicit tsc =>
      val file = new File(runConfiguration.sampleSheet)
      val fileName = file.getName
      logger.debug(s"Fetching sample sheet $file")
      SharedFile(file, fileName).map(SampleSheetFile(_)).andThen {
        case Success(_) =>
          logger.debug(s"Fetched reference")
        case Failure(e) =>
          logger.error(s"Failed to fetch reference $file", e)

      }
    }
  }

  private def fetchTargetIntervals(runConfiguration: RunConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) = {
    tsc.withFilePrefix(Seq("references")) { implicit tsc =>
      val file = new File(runConfiguration.targetIntervals)
      val fileName = file.getName
      logger.debug(s"Fetching target interval file $file")
      SharedFile(file, fileName).map(BedFile(_)).andThen {
        case Success(_) =>
          logger.debug(s"Fetched target intervals (capture kit definition)")
        case Failure(e) =>
          logger.error(s"Failed to target intervals $file", e)

      }
    }
  }
  private def fetchKnownSitesFiles(runConfiguration: RunConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) = {
    val files =
      runConfiguration.bqsrKnownSites

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
    AsyncTask[SingleSamplePipelineInput, SingleSamplePipelineResult](
      "__persample-single",
      1) {
      case SingleSamplePipelineInput(demultiplexed,
                                     knownSites,
                                     indexedReference,
                                     selectionTargetIntervals) =>
        implicit computationEnvironment =>
          log.info(s"Processing demultiplexed sample $demultiplexed")
          releaseResources

          def intoIntermediateFolder[T] =
            appendToFilePrefix[T](
              Seq(demultiplexed.project, demultiplexed.runId, "intermediate"))

          def intoFinalFolder[T] =
            appendToFilePrefix[T](
              Seq(demultiplexed.project, demultiplexed.runId))

          def intoQCFolder[T] =
            appendToFilePrefix[T](
              Seq(demultiplexed.project, demultiplexed.runId, "QC"))

          for {

            MarkDuplicateResult(alignedSample, duplicationQC) <- intoIntermediateFolder {
              implicit computationEnvironment =>
                BWAAlignment
                  .alignFastqPerSample(
                    PerSampleBWAAlignmentInput(demultiplexed.lanes,
                                               demultiplexed.project,
                                               demultiplexed.sampleId,
                                               demultiplexed.runId,
                                               indexedReference))(
                    ResourceConfig.minimal)
            }

            coordinateSorted <- intoIntermediateFolder {
              implicit computationEnvironment =>
                BWAAlignment.sortByCoordinateAndIndex(alignedSample.bam)(
                  ResourceConfig.sortBam)
            }

            _ <- alignedSample.bam.file.delete

            table <- intoIntermediateFolder { implicit computationEnvironment =>
              BaseQualityScoreRecalibration.trainBQSR(
                TrainBQSRInput(coordinateSorted,
                               indexedReference,
                               knownSites.toSet))(ResourceConfig.trainBqsr)
            }
            recalibrated <- intoFinalFolder { implicit computationEnvironment =>
              BaseQualityScoreRecalibration.applyBQSR(
                ApplyBQSRInput(coordinateSorted, indexedReference, table))(
                ResourceConfig.applyBqsr)
            }

            alignmentQC <- intoQCFolder { implicit computationEnvironment =>
              AlignmentQC.general(
                AlignmentQCInput(recalibrated, indexedReference))(
                ResourceConfig.minimal)
            }
            targetSelectionQC <- intoQCFolder {
              implicit computationEnvironment =>
                AlignmentQC.hybridizationSelection(
                  SelectionQCInput(recalibrated,
                                   indexedReference,
                                   selectionTargetIntervals))(
                  ResourceConfig.minimal)
            }

          } yield
            SingleSamplePipelineResult(
              bam = recalibrated,
              project = demultiplexed.project,
              runId = demultiplexed.runId,
              sampleId = demultiplexed.sampleId,
              alignmentQC = alignmentQC,
              duplicationQC = duplicationQC,
              targetSelectionQC = targetSelectionQC
            )

    }

  val allSamples =
    AsyncTask[PerSamplePipelineInput, PerSamplePipelineResult](
      "__persample-allsamples",
      1) {
      case PerSamplePipelineInput(demultiplexed,
                                  referenceFasta,
                                  knownSites,
                                  selectionTargetIntervals) =>
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
                                                indexedFasta,
                                                selectionTargetIntervals))(
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

object SingleSamplePipelineResult {
  implicit val encoder: Encoder[SingleSamplePipelineResult] =
    deriveEncoder[SingleSamplePipelineResult]
  implicit val decoder: Decoder[SingleSamplePipelineResult] =
    deriveDecoder[SingleSamplePipelineResult]
}
