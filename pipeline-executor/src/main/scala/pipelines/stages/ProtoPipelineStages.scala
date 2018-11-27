package org.gc.pipelines.stages

import scala.concurrent.{ExecutionContext, Future}
import tasks._
import tasks.circesupport._
import org.gc.pipelines.application.{
  RunfolderReadyForProcessing,
  RunConfiguration,
  Selector
}
import org.gc.pipelines.model._
import org.gc.pipelines.util.ResourceConfig
import java.io.File
import com.typesafe.scalalogging.StrictLogging
import scala.util.{Success, Failure}

object ProtoPipelineStages extends StrictLogging {

  val singleSampleWES =
    AsyncTask[SingleSamplePipelineInput, SingleSamplePipelineResult](
      "__persample-single",
      1) {
      case SingleSamplePipelineInput(demultiplexed,
                                     referenceFasta,
                                     knownSites,
                                     selectionTargetIntervals,
                                     dbSnpVcf,
                                     variantEvaluationIntervals,
                                     bamOfPreviousRuns,
                                     vqsrTrainingFiles) =>
        implicit computationEnvironment =>
          log.info(s"Processing demultiplexed sample $demultiplexed")
          releaseResources

          val runIdTag =
            demultiplexed.runIdTag

          def intoIntermediateFolder[T] =
            appendToFilePrefix[T](
              Seq("projects",
                  demultiplexed.project,
                  demultiplexed.sampleId,
                  runIdTag,
                  "intermediate"))

          def intoFinalFolder[T] =
            appendToFilePrefix[T](
              Seq("projects",
                  demultiplexed.project,
                  demultiplexed.sampleId,
                  runIdTag))

          def intoQCFolder[T] =
            appendToFilePrefix[T](
              Seq("projects",
                  demultiplexed.project,
                  demultiplexed.sampleId,
                  runIdTag,
                  "QC"))

          for {

            indexedReference <- BWAAlignment.indexReference(referenceFasta)(
              ResourceConfig.indexReference)

            MarkDuplicateResult(alignedSample, duplicationQC) <- intoIntermediateFolder {
              implicit computationEnvironment =>
                BWAAlignment
                  .alignFastqPerSample(
                    PerSampleBWAAlignmentInput(demultiplexed.lanes,
                                               demultiplexed.project,
                                               demultiplexed.sampleId,
                                               indexedReference,
                                               bamOfPreviousRuns))(
                    ResourceConfig.minimal)
            }

            coordinateSorted <- intoIntermediateFolder {
              implicit computationEnvironment =>
                BWAAlignment.sortByCoordinateAndIndex(alignedSample.bam)(
                  ResourceConfig.sortBam)
            }

            // _ <- alignedSample.bam.file.delete

            bqsrTable <- intoIntermediateFolder {
              implicit computationEnvironment =>
                BaseQualityScoreRecalibration.trainBQSR(
                  TrainBQSRInput(coordinateSorted,
                                 indexedReference,
                                 knownSites.toSet))(ResourceConfig.trainBqsr)
            }
            recalibrated <- intoFinalFolder { implicit computationEnvironment =>
              BaseQualityScoreRecalibration.applyBQSR(
                ApplyBQSRInput(coordinateSorted, indexedReference, bqsrTable))(
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
            wgsQC <- intoQCFolder { implicit computationEnvironment =>
              AlignmentQC.wholeGenomeMetrics(
                CollectWholeGenomeMetricsInput(recalibrated, indexedReference))(
                ResourceConfig.minimal)
            }

            haplotypeCallerReferenceCalls <- intoFinalFolder {
              implicit computationEnvironment =>
                HaplotypeCaller.haplotypeCaller(
                  HaplotypeCallerInput(recalibrated, indexedReference))(
                  ResourceConfig.minimal)
            }

            GenotypeGVCFsResult(_, genotypesScattered) <- intoIntermediateFolder {
              implicit computationEnvironment =>
                HaplotypeCaller.genotypeGvcfs(
                  GenotypeGVCFsInput(
                    Set(haplotypeCallerReferenceCalls),
                    indexedReference,
                    dbSnpVcf,
                    demultiplexed.project + "." + demultiplexed.sampleId + ".single",
                    vqsrTrainingFiles = vqsrTrainingFiles
                  ))(ResourceConfig.minimal)
            }

            gvcf <- intoFinalFolder { implicit computationEnvironment =>
              HaplotypeCaller.mergeVcfs(MergeVCFInput(
                genotypesScattered,
                demultiplexed.project + "." + demultiplexed.sampleId + ".single.genotyped.vcf.gz"))(
                ResourceConfig.minimal)
            }

            gvcfQC <- intoQCFolder { implicit computationEnvironment =>
              HaplotypeCaller.collectVariantCallingMetrics(
                CollectVariantCallingMetricsInput(indexedReference,
                                                  gvcf,
                                                  dbSnpVcf,
                                                  variantEvaluationIntervals))(
                ResourceConfig.minimal)
            }

          } yield
            SingleSamplePipelineResult(
              bam = recalibrated,
              uncalibrated = alignedSample.bam,
              haplotypeCallerReferenceCalls = haplotypeCallerReferenceCalls,
              gvcf = gvcf,
              project = demultiplexed.project,
              sampleId = demultiplexed.sampleId,
              alignmentQC = alignmentQC,
              duplicationQC = duplicationQC,
              targetSelectionQC = targetSelectionQC,
              wgsQC = wgsQC,
              gvcfQC = gvcfQC
            )

    }

  val singleSampleRNA =
    AsyncTask[SingleSamplePipelineInputRNASeq, SingleSamplePipelineResultRNA](
      "__rna-persample-allsamples",
      1) {
      case SingleSamplePipelineInputRNASeq(demultiplexed,
                                           referenceFasta,
                                           gtf,
                                           readLengths) =>
        implicit computationEnvironment =>
          releaseResources
          computationEnvironment.withFilePrefix(Seq("projects")) {
            implicit computationEnvironment =>
              def inProjectFolder[T](sample: PerSampleFastQ) =
                appendToFilePrefix[T](
                  Seq(sample.project, sample.sampleId, sample.runIdTag))

              for {
                indexedFasta <- StarAlignment.indexReference(referenceFasta)(
                  ResourceConfig.createStarIndex)

                processedSample <- inProjectFolder(demultiplexed) {
                  implicit computationEnvironment =>
                    for {
                      starResult <- StarAlignment.alignSample(
                        StarAlignmentInput(
                          fastqs = demultiplexed.lanes,
                          project = demultiplexed.project,
                          sampleId = demultiplexed.sampleId,
                          runId = demultiplexed.lanes.head.runId,
                          reference = indexedFasta,
                          gtf = gtf.file,
                          readLength = readLengths.map(_._2).max
                        ))(ResourceConfig.starAlignment)
                    } yield SingleSamplePipelineResultRNA(starResult)
                }

              } yield processedSample
          }

    }

  def select(selector: Selector, sample: PerSamplePerRunFastQ) =
    Option(sample).filter { sample =>
      val lanes = sample.lanes.map { fqLane =>
        Metadata(fqLane.runId, fqLane.lane, sample.sampleId, sample.project)
      }
      lanes.exists(selector.isSelected)

    }

  def parseReadLengthFromRunInfo(
      run: RunfolderReadyForProcessing): Either[String, Map[ReadType, Int]] = {
    val file = new File(run.runFolderPath + "/RunInfo.xml")
    if (file.canRead) {
      val content = fileutils.openSource(
        new File(run.runFolderPath + "/RunInfo.xml"))(_.mkString)
      Right(parseReadLength(content))
    } else Left("Can't read " + file)
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

  def executeDemultiplexing(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    Future
      .traverse(r.runConfiguration.demultiplexingRuns.toSeq) {
        demultiplexingConfig =>
          inDemultiplexingFolder(r.runId, demultiplexingConfig.demultiplexingId) {
            implicit tsc =>
              for {
                globalIndexSet <- ProtoPipelineStages.fetchGlobalIndexSet(
                  r.runConfiguration)
                sampleSheet <- ProtoPipelineStages.fetchSampleSheet(
                  demultiplexingConfig.sampleSheet)

                demultiplexed <- Demultiplexing.allLanes(
                  DemultiplexingInput(
                    r.runFolderPath,
                    sampleSheet,
                    demultiplexingConfig.extraBcl2FastqArguments,
                    globalIndexSet))(ResourceConfig.minimal)

                perSampleFastQs = ProtoPipelineStages
                  .groupBySample(demultiplexed.withoutUndetermined,
                                 demultiplexingConfig.readAssignment,
                                 demultiplexingConfig.umi,
                                 r.runId)

              } yield perSampleFastQs

          }
      }
      .map(_.flatten)

  private def inDemultiplexingFolder[T](runId: RunId,
                                        demultiplexingId: DemultiplexingId)(
      f: TaskSystemComponents => T)(implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("demultiplexing", runId, demultiplexingId))(f)

  def fetchReference(runConfiguration: RunConfiguration)(
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

  def fetchSampleSheet(path: String)(implicit tsc: TaskSystemComponents,
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

  def fetchGlobalIndexSet(runConfiguration: RunConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    runConfiguration.globalIndexSet match {
      case None       => Future.successful(None)
      case Some(path) => fetchFile("references", path).map(Some(_))
    }
  def fetchGenemodel(runConfiguration: RunConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    fetchFile("references", runConfiguration.geneModelGtf).map(GTFFile(_))

  def fetchVariantEvaluationIntervals(runConfiguration: RunConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    fetchFile("references", runConfiguration.variantEvaluationIntervals)
      .map(BedFile(_))

  def fetchDbSnpVcf(runConfiguration: RunConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    for {
      vcf <- fetchFile("references", runConfiguration.dbSnpVcf)
      vcfidx <- fetchFile("references", runConfiguration.dbSnpVcf + ".tbi")
    } yield VCF(vcf, Some(vcfidx))

  def fetchVqsrTrainingFiles(runConfiguration: RunConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    if (runConfiguration.vqsrMillsAnd1Kg.isDefined)
      for {
        millsAnd1Kg <- fetchFile("references",
                                 runConfiguration.vqsrMillsAnd1Kg.get)
        millsAnd1KgIdx <- fetchFile(
          "references",
          runConfiguration.vqsrMillsAnd1Kg.get + ".tbi")
        oneKg <- fetchFile("references",
                           runConfiguration.vqsrOneKgHighConfidenceSnps.get)
        oneKgIdx <- fetchFile(
          "references",
          runConfiguration.vqsrOneKgHighConfidenceSnps.get + ".tbi")
        hapmap <- fetchFile("references", runConfiguration.vqsrHapmap.get)
        hapmapIdx <- fetchFile("references",
                               runConfiguration.vqsrHapmap.get + ".tbi")
        omni <- fetchFile("references", runConfiguration.vqsrOneKgOmni.get)
        omniIdx <- fetchFile("references",
                             runConfiguration.vqsrOneKgOmni.get + ".tbi")
        dbSnp138 <- fetchFile("references", runConfiguration.vqsrDbSnp138.get)
        dbSnp138Idx <- fetchFile("references",
                                 runConfiguration.vqsrDbSnp138.get + ".tbi")
      } yield
        Some(
          VQSRTrainingFiles(
            millsAnd1Kg = VCF(millsAnd1Kg, Some(millsAnd1KgIdx)),
            oneKgHighConfidenceSnps = VCF(oneKg, Some(oneKgIdx)),
            hapmap = VCF(hapmap, Some(hapmapIdx)),
            oneKgOmni = VCF(omni, Some(omniIdx)),
            dbSnp138 = VCF(dbSnp138, Some(dbSnp138Idx))
          ))
    else Future.successful(None)

  def fetchFile(folderName: String, path: String)(
      implicit tsc: TaskSystemComponents) = {
    tsc.withFilePrefix(Seq(folderName)) { implicit tsc =>
      val file = new File(path)
      val fileName = file.getName
      logger.debug(s"Fetching $file")
      SharedFile(file, fileName)
    }
  }

  def fetchTargetIntervals(runConfiguration: RunConfiguration)(
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
  def fetchKnownSitesFiles(runConfiguration: RunConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) = {
    val files =
      runConfiguration.bqsrKnownSites

    val fileListWithIndices = files.map { vcfFile =>
      (new File(vcfFile), new File(vcfFile + ".tbi"))
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
                    runId: RunId): Seq[PerSamplePerRunFastQ] =
    demultiplexed.fastqs
      .groupBy { fq =>
        (fq.project, fq.sampleId)
      }
      .toSeq
      .map {
        case ((project, sampleId), perSampleFastQs) =>
          val perLaneFastQs =
            perSampleFastQs
              .groupBy(s => (s.lane, s.partition))
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

                val partition = {
                  val distinctPartitionsInGroup = fqsInLane.map(_.partition)
                  assert(distinctPartitionsInGroup.size == 1) // due to groupBy
                  distinctPartitionsInGroup.head
                }

                for {
                  read1 <- maybeRead1
                  read2 <- maybeRead2
                } yield
                  FastQPerLane(runId, lane, read1, read2, maybeUmi, partition)
              }
              .flatten
          PerSamplePerRunFastQ(
            perLaneFastQs.toSet,
            project,
            sampleId,
            runId
          )
      }

  def selectReadType(fqs: Seq[FastQWithSampleMetadata], readType: ReadType) =
    fqs
      .filter(_.readType == readType)
      .headOption
      .map(_.fastq)

}
