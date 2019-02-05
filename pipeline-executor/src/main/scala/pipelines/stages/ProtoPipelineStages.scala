package org.gc.pipelines.stages

import scala.concurrent.{ExecutionContext, Future}
import tasks._
import tasks.circesupport._
import org.gc.pipelines.application.{
  RunfolderReadyForProcessing,
  RunConfiguration,
  WESConfiguration,
  RNASeqConfiguration,
  Selector
}
import org.gc.pipelines.model._
import org.gc.pipelines.util.ResourceConfig
import org.gc.pipelines.util.StableSet
import org.gc.pipelines.util.StableSet.syntax
import java.io.File
import com.typesafe.scalalogging.StrictLogging
import scala.util.{Success, Failure}

object ProtoPipelineStages extends StrictLogging {

  val singleSampleWES =
    AsyncTask[SingleSamplePipelineInput, SingleSamplePipelineResult](
      "__persample-single",
      1) {
      case SingleSamplePipelineInput(analysisId,
                                     demultiplexed,
                                     referenceFasta,
                                     knownSites,
                                     selectionTargetIntervals,
                                     dbSnpVcf,
                                     variantEvaluationIntervals,
                                     bamOfPreviousRuns,
                                     doVariantCalling) =>
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
                  analysisId,
                  "intermediate").filter(_.nonEmpty))

          def intoFinalFolder[T] =
            appendToFilePrefix[T](
              Seq("projects",
                  demultiplexed.project,
                  demultiplexed.sampleId,
                  runIdTag,
                  analysisId).filter(_.nonEmpty))

          def intoQCFolder[T] =
            appendToFilePrefix[T](
              Seq("projects",
                  demultiplexed.project,
                  demultiplexed.sampleId,
                  runIdTag,
                  analysisId,
                  "QC").filter(_.nonEmpty))

          def intoCoverageFolder[T] =
            appendToFilePrefix[T](
              Seq("coverages", demultiplexed.project, analysisId)
                .filter(_.nonEmpty))

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
                                 knownSites))(ResourceConfig.trainBqsr)
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

            _ <- intoCoverageFolder { implicit computationEnvironment =>
              AlignmentQC.parseWholeGenomeMetrics(
                ParseWholeGenomeCoverageInput(wgsQC,
                                              demultiplexed.runIdTag,
                                              demultiplexed.project,
                                              demultiplexed.sampleId,
                                              analysisId))(
                ResourceConfig.minimal)
            }

            variantCalls <- if (!doVariantCalling) Future.successful(None)
            else {
              for {
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
                        StableSet(haplotypeCallerReferenceCalls),
                        indexedReference,
                        dbSnpVcf,
                        demultiplexed.project + "." + demultiplexed.sampleId + ".single",
                        vqsrTrainingFiles = None
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
                    CollectVariantCallingMetricsInput(
                      indexedReference,
                      gvcf,
                      dbSnpVcf,
                      variantEvaluationIntervals))(ResourceConfig.minimal)
                }
              } yield Some((haplotypeCallerReferenceCalls, gvcf, gvcfQC))
            }

          } yield
            SingleSamplePipelineResult(
              bam = recalibrated,
              uncalibrated = alignedSample.bam,
              haplotypeCallerReferenceCalls = variantCalls.map(_._1),
              gvcf = variantCalls.map(_._2),
              project = demultiplexed.project,
              sampleId = demultiplexed.sampleId,
              alignmentQC = alignmentQC,
              duplicationQC = duplicationQC,
              targetSelectionQC = targetSelectionQC,
              wgsQC = wgsQC,
              gvcfQC = variantCalls.map(_._3),
              analysisId = analysisId
            )

    }

  val singleSampleRNA =
    AsyncTask[SingleSamplePipelineInputRNASeq, SingleSamplePipelineResultRNA](
      "__rna-persample-allsamples",
      1) {
      case SingleSamplePipelineInputRNASeq(analysisId,
                                           demultiplexed,
                                           referenceFasta,
                                           gtf,
                                           readLengths) =>
        implicit computationEnvironment =>
          releaseResources
          computationEnvironment.withFilePrefix(Seq("projects")) {
            implicit computationEnvironment =>
              def inProjectFolder[T] =
                appendToFilePrefix[T](
                  Seq(demultiplexed.project,
                      demultiplexed.sampleId,
                      demultiplexed.runIdTag,
                      analysisId).filter(_.nonEmpty))

              for {
                indexedFasta <- StarAlignment.indexReference(referenceFasta)(
                  ResourceConfig.createStarIndex)

                processedSample <- inProjectFolder {
                  implicit computationEnvironment =>
                    for {
                      starResult <- StarAlignment.alignSample(
                        StarAlignmentInput(
                          fastqs = demultiplexed.lanes,
                          project = demultiplexed.project,
                          sampleId = demultiplexed.sampleId,
                          runId = demultiplexed.lanes.toSeq.head.runId,
                          reference = indexedFasta,
                          gtf = gtf.file,
                          readLength = readLengths.map(_._2).toSeq.max
                        ))(ResourceConfig.starAlignment)
                      coordinateSorted <- BWAAlignment.sortByCoordinateAndIndex(
                        starResult.bam.bam)(ResourceConfig.sortBam)
                      counts <- QTLToolsQuantification.quantify(
                        QTLToolsQuantificationInput(
                          coordinateSorted,
                          gtf,
                          Nil
                        ))(ResourceConfig.qtlToolsQuantification)
                    } yield
                      SingleSamplePipelineResultRNA(analysisId,
                                                    starResult,
                                                    counts)
                }

              } yield processedSample
          }

    }

  def selectConfiguration[A](selectors: List[(Selector, A)],
                             sample: PerSamplePerRunFastQ): Option[A] =
    selectors
      .find {
        case (selector, _) =>
          val lanes = sample.lanes.map { fqLane =>
            Metadata(fqLane.runId, fqLane.lane, sample.sampleId, sample.project)
          }
          lanes.toSeq.exists(selector.isSelected)
      }
      .map(_._2)

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
                  demultiplexingConfig.isTenX,
                  demultiplexingConfig.sampleSheet)
                parsedSampleSheet <- sampleSheet.parse

                demultiplexed <- Demultiplexing.allLanes(DemultiplexingInput(
                  r.runFolderPath,
                  sampleSheet,
                  demultiplexingConfig.extraBcl2FastqArguments,
                  globalIndexSet,
                  partitionByLane = demultiplexingConfig.partitionByLane,
                  noPartition = demultiplexingConfig.tenX
                ))(ResourceConfig.minimal,
                   labels = ResourceConfig.projectLabel(
                     parsedSampleSheet.projects: _*))

                perSampleFastQs = ProtoPipelineStages
                  .groupBySample(demultiplexed.withoutUndetermined,
                                 demultiplexingConfig.readAssignment,
                                 demultiplexingConfig.umi,
                                 r.runId)

              } yield perSampleFastQs

          }
      }
      .map { demultiplexingRuns =>
        val flattened = demultiplexingRuns.flatten
        flattened
          .groupBy(sample => (sample.project, sample.sampleId))
          .toSeq
          .flatMap {
            case (_, group) =>
              if (group.size > 1) {
                logger.warn(
                  s"The same sample have been demultiplexed several times. Dropping all from further analyses. $group")
                Nil
              } else List(group.head)
          }
      }

  private def inDemultiplexingFolder[T](runId: RunId,
                                        demultiplexingId: DemultiplexingId)(
      f: TaskSystemComponents => T)(implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("demultiplexing", runId, demultiplexingId))(f)

  def fetchReference(filePath: String)(implicit tsc: TaskSystemComponents,
                                       ec: ExecutionContext) = {
    tsc.withFilePrefix(Seq("references")) { implicit tsc =>
      val file = new File(filePath)
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

  def resolve10XIfNeeded(is10X: Boolean, sampleSheet: SampleSheetFile)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    if (!is10X) Future.successful(sampleSheet)
    else
      for {
        parsed <- sampleSheet.parse
        resolved = TenX.resolve(parsed)
        saved <- SharedFile(
          akka.stream.scaladsl.Source
            .single(akka.util.ByteString(resolved.sampleSheetContent)),
          sampleSheet.file.name + ".resolved")
      } yield SampleSheetFile(saved)

  def fetchSampleSheet(is10X: Boolean, path: String)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) = {
    tsc.withFilePrefix(Seq("sampleSheets")) { implicit tsc =>
      val file = new File(path)
      val fileName = file.getName
      logger.debug(s"Fetching sample sheet $file")
      for {
        sampleSheet <- SharedFile(file, fileName)
          .map(SampleSheetFile(_))
          .andThen {
            case Success(_) =>
              logger.debug(s"Fetched sample sheet")
            case Failure(e) =>
              logger.error(s"Failed to fetch sample sheet $file", e)

          }
        resolvedFor10X <- resolve10XIfNeeded(is10X, sampleSheet)
      } yield resolvedFor10X

    }
  }

  def fetchGlobalIndexSet(runConfiguration: RunConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    runConfiguration.globalIndexSet match {
      case None       => Future.successful(None)
      case Some(path) => fetchFile("references", path).map(Some(_))
    }
  def fetchGenemodel(runConfiguration: RNASeqConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    fetchFile("references", runConfiguration.geneModelGtf).map(GTFFile(_))

  def fetchVariantEvaluationIntervals(runConfiguration: WESConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    fetchFile("references", runConfiguration.variantEvaluationIntervals)
      .map(BedFile(_))

  def fetchDbSnpVcf(runConfiguration: WESConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    for {
      vcf <- fetchFile("references", runConfiguration.dbSnpVcf)
      vcfidx <- fetchFile("references", runConfiguration.dbSnpVcf + ".tbi")
    } yield VCF(vcf, Some(vcfidx))

  def fetchVqsrTrainingFiles(runConfiguration: WESConfiguration)(
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

  def fetchTargetIntervals(runConfiguration: WESConfiguration)(
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
  def fetchKnownSitesFiles(runConfiguration: WESConfiguration)(
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
    Future.sequence(vcfFilesFuture.toSeq).andThen {
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
    demultiplexed.fastqs.toSeq
      .groupBy { fq =>
        (fq.project, fq.sampleId)
      }
      .toSeq
      .map {
        case ((project, sampleId), perSampleFastQs) =>
          val perLaneFastQs =
            perSampleFastQs.toSeq
              .groupBy(s => (s.lane, s.partition))
              .toSeq
              .map(_._2.toSet)
              .map { (fqsInLane: Set[FastQWithSampleMetadata]) =>
                val maybeRead1 =
                  selectReadType(fqsInLane.toSeq, ReadType(readAssignment._1))

                val maybeRead2 =
                  selectReadType(fqsInLane.toSeq, ReadType(readAssignment._2))

                val maybeUmi = umi.flatMap(umiReadNumber =>
                  selectReadType(fqsInLane.toSeq, ReadType(umiReadNumber)))

                val lane = {
                  val distinctLanesInGroup = fqsInLane.map(_.lane)
                  require(distinctLanesInGroup.size == 1) // due to groupBy
                  distinctLanesInGroup.head
                }

                val partition = {
                  val distinctPartitionsInGroup = fqsInLane.map(_.partition)
                  require(distinctPartitionsInGroup.size == 1) // due to groupBy
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
            perLaneFastQs.toSet.toStable,
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
