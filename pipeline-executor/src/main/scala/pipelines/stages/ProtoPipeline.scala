package org.gc.pipelines.stages

import scala.concurrent.{ExecutionContext, Future}
import tasks._
import tasks.circesupport._
import org.gc.pipelines.application.{
  Pipeline,
  RunfolderReadyForProcessing,
  RunConfiguration,
  Selector
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

    def inRunQCFolder[T](runId: RunId)(f: TaskSystemComponents => T) =
      tsc.withFilePrefix(Seq("runQC", runId))(f)

    def inDeliverablesFolder[T](f: TaskSystemComponents => T) =
      tsc.withFilePrefix(Seq("deliverables"))(f)

    def inDemultiplexingFolder[T](
        runId: RunId,
        demultiplexingId: DemultiplexingId)(f: TaskSystemComponents => T) =
      tsc.withFilePrefix(Seq("demultiplexing", runId, demultiplexingId))(f)

    def executeDemultiplexing =
      Future
        .traverse(r.runConfiguration.demultiplexingRuns.toSeq) {
          demultiplexingConfig =>
            inDemultiplexingFolder(r.runId,
                                   demultiplexingConfig.demultiplexingId) {
              implicit tsc =>
                for {
                  globalIndexSet <- ProtoPipeline.fetchGlobalIndexSet(
                    r.runConfiguration)
                  sampleSheet <- ProtoPipeline.fetchSampleSheet(
                    demultiplexingConfig.sampleSheet)

                  demultiplexed <- Demultiplexing.allLanes(
                    DemultiplexingInput(
                      r.runFolderPath,
                      sampleSheet,
                      demultiplexingConfig.extraBcl2FastqArguments,
                      globalIndexSet))(ResourceConfig.minimal)

                  perSampleFastQs = ProtoPipeline
                    .groupBySample(demultiplexed.withoutUndetermined,
                                   demultiplexingConfig.readAssignment,
                                   demultiplexingConfig.umi,
                                   r.runId)

                } yield perSampleFastQs

            }
        }
        .map(_.flatten)

    val readLengths = ProtoPipeline.parseReadLengthFromRunInfo(r)

    logger.info(s"${r.runId} read lengths: ${readLengths.mkString(", ")}")

    for {
      reference <- ProtoPipeline.fetchReference(r.runConfiguration)
      knownSites <- ProtoPipeline.fetchKnownSitesFiles(r.runConfiguration)
      gtf <- ProtoPipeline.fetchGenemodel(r.runConfiguration)
      selectionTargetIntervals <- ProtoPipeline.fetchTargetIntervals(
        r.runConfiguration)

      perSampleFastQs <- executeDemultiplexing

      fastpReports = startFastpReports(perSampleFastQs)

      samplesForWESAnalysis = ProtoPipeline.select(
        r.runConfiguration.wesSelector,
        perSampleFastQs)

      samplesForRNASeqAnalysis = ProtoPipeline.select(
        r.runConfiguration.rnaSelector,
        perSampleFastQs)

      _ = {
        logger.info(
          s"Demultiplexed samples from run ${r.runId} : $perSampleFastQs")
        logger.info(s"WES samples from run ${r.runId} : $samplesForWESAnalysis")
        logger.info(
          s"RNASEQ samples from run ${r.runId} : $samplesForRNASeqAnalysis")
      }

      wesAnalysis = ProtoPipeline.allSamplesWES(
        PerSamplePipelineInput(
          samplesForWESAnalysis.toSet,
          reference,
          knownSites.toSet,
          selectionTargetIntervals
        ))(ResourceConfig.minimal)

      rnaSeqAnalysis = ProtoPipeline.allSamplesRNASeq(
        PerSamplePipelineInputRNASeq(
          samplesForRNASeqAnalysis.toSet,
          reference,
          gtf,
          readLengths.toSeq.toSet
        ))(ResourceConfig.minimal)

      perSampleResultsWES <- wesAnalysis
      perSampleResultsRNA <- rnaSeqAnalysis

      fastpReports <- fastpReports

      sampleQCsWES = extractQCFiles(perSampleResultsWES, fastpReports)

      _ <- inRunQCFolder(r.runId) { implicit tsc =>
        AlignmentQC.runQCTable(RunQCTableInput(RunId(r.runId), sampleQCsWES))(
          ResourceConfig.minimal)
      }
      _ <- inRunQCFolder(r.runId) { implicit tsc =>
        RunQCRNA.runQCTable(
          RunQCTableRNAInput(r.runId, perSampleResultsRNA.samples))(
          ResourceConfig.minimal)
      }

      _ <- inDeliverablesFolder { implicit tsc =>
        Delivery.collectDeliverables(
          CollectDeliverablesInput(
            r.runId,
            perSampleFastQs.toSet,
            perSampleResultsWES.samples,
            perSampleResultsRNA.samples
          ))(ResourceConfig.minimal)
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
      tsc.withFilePrefix(Seq("fastp", fq.runId, fq.lane.lane.toString)) {
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

case class PerSamplePipelineInputRNASeq(demultiplexed: Set[PerSampleFastQ],
                                        reference: ReferenceFasta,
                                        gtf: GTFFile,
                                        readLengths: Set[(ReadType, Int)])
    extends WithSharedFiles(
      demultiplexed.toSeq.flatMap(_.files) ++ reference.files ++ gtf.files: _*)

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

case class PerSamplePipelineResultRNASeq(samples: Set[StarResult])
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
                    umi: Option[Int],
                    runId: RunId): Seq[PerSampleFastQ] =
    demultiplexed.fastqs
      .groupBy { fq =>
        (fq.project, fq.sampleId)
      }
      .toSeq
      .map {
        case ((project, sampleId), perSampleFastQs) =>
          val perLaneFastQs =
            perSampleFastQs
              .groupBy(_.lane)
              .toSeq
              .map(_._2)
              .map { (fqsInLane: Set[FastQWithSampleMetadata]) =>
                val maybeRead1 =
                  selectReadType(fqsInLane.toSeq, ReadType(readAssignment._1))

                val maybeRead2 =
                  selectReadType(fqsInLane.toSeq, ReadType(readAssignment._2))

                val maybeUmi = umi.flatMap(umiReadNumber =>
                  selectReadType(fqsInLane.toSeq, ReadType(umiReadNumber)))

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

  private def fetchSampleSheet(path: String)(implicit tsc: TaskSystemComponents,
                                             ec: ExecutionContext) = {
    tsc.withFilePrefix(Seq("sampleSheets")) { implicit tsc =>
      val file = new File(path)
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

  private def fetchGlobalIndexSet(runConfiguration: RunConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    runConfiguration.globalIndexSet match {
      case None       => Future.successful(None)
      case Some(path) => fetchFile("references", path).map(Some(_))
    }
  private def fetchGenemodel(runConfiguration: RunConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    fetchFile("references", runConfiguration.geneModelGtf).map(GTFFile(_))

  private def fetchFile(folderName: String, path: String)(
      implicit tsc: TaskSystemComponents) = {
    tsc.withFilePrefix(Seq(folderName)) { implicit tsc =>
      val file = new File(path)
      val fileName = file.getName
      logger.debug(s"Fetching $file")
      SharedFile(file, fileName)
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

  val singleSampleWES =
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
            _ <- coordinateSorted.bam.delete
            _ <- coordinateSorted.bai.delete

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

  val allSamplesWES =
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
                    ProtoPipeline.singleSampleWES(
                      SingleSamplePipelineInput(perSampleFastQs,
                                                knownSites,
                                                indexedFasta,
                                                selectionTargetIntervals))(
                      ResourceConfig.minimal)
                  }

              } yield PerSamplePipelineResult(processedSamples.toSet)
          }

    }

  val allSamplesRNASeq =
    AsyncTask[PerSamplePipelineInputRNASeq, PerSamplePipelineResultRNASeq](
      "__rna-persample-allsamples",
      1) {
      case PerSamplePipelineInputRNASeq(demultiplexed,
                                        referenceFasta,
                                        gtf,
                                        readLengths) =>
        implicit computationEnvironment =>
          releaseResources
          computationEnvironment.withFilePrefix(Seq("projects")) {
            implicit computationEnvironment =>
              val allLanesOfAllSamples = demultiplexed.toSeq.flatMap {
                perSampleFastQs =>
                  perSampleFastQs.lanes.map(lane => (perSampleFastQs, lane))
              }

              def inProjectFolder[T](project: Project, run: RunId) =
                appendToFilePrefix[T](Seq(project, run))

              if (allLanesOfAllSamples.isEmpty)
                Future.successful(PerSamplePipelineResultRNASeq(Set.empty))
              else
                for {
                  indexedFasta <- StarAlignment.indexReference(referenceFasta)(
                    ResourceConfig.createStarIndex)

                  processedSamples <- Future
                    .traverse(allLanesOfAllSamples) {
                      case (meta, lane) =>
                        inProjectFolder(meta.project, meta.runId) {
                          implicit computationEnvironment =>
                            StarAlignment.alignSingleLane(
                              PerLaneStarAlignmentInput(
                                read1 = lane.read1,
                                read2 = lane.read2,
                                project = meta.project,
                                sampleId = meta.sampleId,
                                runId = meta.runId,
                                lane = lane.lane,
                                reference = indexedFasta,
                                gtf = gtf.file,
                                readLength = readLengths.map(_._2).max
                              ))(ResourceConfig.starAlignment)
                        }

                    }

                } yield PerSamplePipelineResultRNASeq(processedSamples.toSet)
          }

    }

  def select(selector: Selector, samples: Seq[PerSampleFastQ]) =
    samples.filter { sample =>
      val lanes = sample.lanes.map { fqLane =>
        Metadata(sample.runId, fqLane.lane, sample.sampleId, sample.project)
      }
      lanes.exists(selector.isSelected)

    }

  def parseReadLengthFromRunInfo(
      run: RunfolderReadyForProcessing): Map[ReadType, Int] = {
    val content = fileutils.openSource(
      new File(run.runFolderPath + "/RunInfo.xml"))(_.mkString)
    parseReadLength(content)
  }

  def parseReadLength(s: String) = {
    val root = scala.xml.XML.loadString(s)
    (root \ "Run" \ "Reads" \ "Read")
      .map { node =>
        (node \@ "Number", node \@ "NumCycles")
      }
      .map {
        case (number, cycles) =>
          ReadType(number.toInt) -> cycles.toInt
      }
      .toMap

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

object PerSamplePipelineInputRNASeq {
  implicit val encoder: Encoder[PerSamplePipelineInputRNASeq] =
    deriveEncoder[PerSamplePipelineInputRNASeq]
  implicit val decoder: Decoder[PerSamplePipelineInputRNASeq] =
    deriveDecoder[PerSamplePipelineInputRNASeq]
}

object PerSamplePipelineResultRNASeq {
  implicit val encoder: Encoder[PerSamplePipelineResultRNASeq] =
    deriveEncoder[PerSamplePipelineResultRNASeq]
  implicit val decoder: Decoder[PerSamplePipelineResultRNASeq] =
    deriveDecoder[PerSamplePipelineResultRNASeq]
}
