package org.gc.pipelines.stages

import scala.concurrent.{ExecutionContext, Future}
import tasks._
import tasks.circesupport._
import tasks.shared.Priority
import org.gc.pipelines.application.{
  RunfolderReadyForProcessing,
  RunConfiguration,
  WESConfiguration,
  RNASeqConfiguration
}
import org.gc.pipelines.model._
import org.gc.pipelines.application.ProgressServer
import org.gc.pipelines.application.ProgressData._
import org.gc.pipelines.util.ResourceConfig
import org.gc.pipelines.util.StableSet
import org.gc.pipelines.util.StableSet.syntax
import java.io.File
import com.typesafe.scalalogging.StrictLogging
import scala.util.{Success, Failure}

object ProtoPipelineStages extends StrictLogging {

  def executeVariantCalling(
      doVariantCalling: Boolean,
      wgsCoverage: Double,
      targetedCoverage: Double,
      minimumWGSCoverage: Option[Double],
      minimumTargetedCoverage: Option[Double]
  ): Boolean = {
    if (!doVariantCalling) false
    else
      minimumWGSCoverage.forall(_ <= wgsCoverage) &&
      minimumTargetedCoverage.forall(_ <= targetedCoverage)

  }

  val singleSampleWES =
    AsyncTask[SingleSamplePipelineInput, SingleSamplePipelineResult](
      "__persample-single",
      6) {
      case SingleSamplePipelineInput(analysisId,
                                     demultiplexed,
                                     referenceFasta,
                                     knownSites,
                                     selectionTargetIntervals,
                                     dbSnpVcf,
                                     variantEvaluationIntervals,
                                     previousAlignedBams,
                                     doVariantCalling,
                                     minimumWGSCoverage,
                                     minimumTargetedCoverage,
                                     contigsFile,
                                     vqsrTrainingFiles,
                                     keepVcf) =>
        implicit computationEnvironment =>
          log.info(s"Processing demultiplexed sample $demultiplexed")
          releaseResources

          val projectPriority =
            ResourceConfig.projectPriority(demultiplexed.project)

          val priorityCoverage = Priority(5000 + projectPriority)
          val priorityBam = Priority(10000 + projectPriority)
          val priorityVcf = Priority(20000 + projectPriority)

          val runIdTag =
            demultiplexed.runIdTag

          def intoRunIntermediateFolder[T] =
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
                  analysisId).filter(_.nonEmpty))

          def intoQCFolder[T] =
            appendToFilePrefix[T](
              Seq("projects",
                  demultiplexed.project,
                  demultiplexed.sampleId,
                  analysisId,
                  "QC").filter(_.nonEmpty))

          def intoCoverageFolder[T] =
            appendToFilePrefix[T](
              Seq("coverages", demultiplexed.project, analysisId)
                .filter(_.nonEmpty))

          def variantDiscovery(
              doVariantCalling: Boolean,
              wgsMeanCoverage: Double,
              targetedMeanCoverage: Double,
              minimumWGSCoverage: Option[Double],
              minimumTargetedCoverage: Option[Double],
              bam: CoordinateSortedBam,
              indexedReference: IndexedReferenceFasta,
              contigsFile: Option[ContigsFile],
              dbSnpVcf: VCF,
              project: Project,
              sampleId: SampleId,
              runIdTag: String,
              analysisId: AnalysisId,
              variantEvaluationIntervals: BedFile,
              vqsrTrainingFiles: Option[VQSRTrainingFiles],
              priorityVcf: Priority,
              keepVcf: Boolean
          ) =
            if (!executeVariantCalling(
                  doVariantCalling = doVariantCalling,
                  wgsCoverage = wgsMeanCoverage,
                  targetedCoverage = targetedMeanCoverage,
                  minimumTargetedCoverage = minimumTargetedCoverage,
                  minimumWGSCoverage = minimumWGSCoverage
                ))
              Future.successful(None)
            else {
              for {
                calls <- intoFinalFolder { implicit computationEnvironment =>
                  HaplotypeCaller.singleSampleVariantDiscovery(
                    SingleSampleVariantDiscoveryInput(
                      bam = bam,
                      indexedReference = indexedReference,
                      contigsFile = contigsFile,
                      dbSnpVcf = dbSnpVcf,
                      project = project,
                      sampleId = sampleId,
                      variantEvaluationIntervals = variantEvaluationIntervals,
                      vqsrTrainingFiles = vqsrTrainingFiles,
                      keepVcf = keepVcf
                    )
                  )(ResourceConfig.minimal, priorityVcf)
                }
                // progress reporting (side effect) from here on
                genotypedVcfPath <- calls.genotypedVcf
                  .map(
                    _.vcf.uri
                      .map(_.toString)
                      .map(Some(_)))
                  .getOrElse(Future.successful(None))
                _ = genotypedVcfPath.foreach { genotypedVcfPath =>
                  ProgressServer.send(
                    VCFAvailable(project,
                                 sampleId,
                                 runIdTag,
                                 analysisId,
                                 genotypedVcfPath))
                }
              } yield Some(calls)

            }

          for {

            indexedReference <- BWAAlignment.indexReference(referenceFasta)(
              ResourceConfig.indexReference)

            PerSampleBWAAlignmentResult(alignedLanes) <- intoRunIntermediateFolder {
              implicit computationEnvironment =>
                BWAAlignment
                  .alignFastqPerSample(
                    PerSampleBWAAlignmentInput(
                      demultiplexed.lanes.map(_.withoutReadLength),
                      demultiplexed.project,
                      demultiplexed.sampleId,
                      indexedReference))(ResourceConfig.minimal,
                                         priorityCoverage)
            }

            allAlignedLanes = (alignedLanes.toSeq ++ previousAlignedBams.toSeq).distinct

            mappedBasesFromPreviousAlignedBams <- Future.traverse(
              previousAlignedBams.toSeq.map(_.bam)) { bam =>
              FastCoverage.countMappedBases(
                CountMappedBasesInput(bam, selectionTargetIntervals))(
                ResourceConfig.minimal,
                priorityCoverage)
            }
            mappedBasesFromThisRun <- Future.traverse(
              alignedLanes.toSeq.map(_.bam)) { bam =>
              FastCoverage.countMappedBases(
                CountMappedBasesInput(bam, selectionTargetIntervals))(
                ResourceConfig.minimal,
                priorityCoverage)
            }

            coverageTotal <- FastCoverage.computeCoverage(
              BamCoverageInput(
                (mappedBasesFromThisRun ++ mappedBasesFromPreviousAlignedBams).toList,
                indexedReference,
                selectionTargetIntervals))(ResourceConfig.minimal,
                                           priorityCoverage)

            coverageThisRun <- FastCoverage.computeCoverage(
              BamCoverageInput(mappedBasesFromThisRun.toList,
                               indexedReference,
                               selectionTargetIntervals))(
              ResourceConfig.minimal,
              priorityCoverage)

            _ <- intoCoverageFolder { implicit computationEnvironment =>
              FastCoverage.writeCoverageToFile(
                WriteCoverageInput(coverageThisRun,
                                   demultiplexed.runIdTag,
                                   demultiplexed.project,
                                   demultiplexed.sampleId,
                                   analysisId))(ResourceConfig.minimal,
                                                priorityBam)
            }

            _ = ProgressServer.send(
              FastCoverageAvailable(demultiplexed.project,
                                    demultiplexed.sampleId,
                                    runIdTag,
                                    analysisId,
                                    coverageThisRun.all))

            mergedResult <- if (!coverageTotal.reachedCoverageTarget(
                                  minimumTargetedCoverage =
                                    minimumTargetedCoverage,
                                  minimumWGSCoverage = minimumWGSCoverage)) {
              logger.info(
                s"$runIdTag ${demultiplexed.project} ${demultiplexed.sampleId} $analysisId has coverage $coverageTotal which is low (needs targeted $minimumTargetedCoverage /wgs $minimumWGSCoverage). Shortcut processing. (minimumWGSCoverage = $minimumWGSCoverage, minimumTargetedCoverage = $minimumTargetedCoverage)")
              Future.successful(None)
            } else {
              logger.info(
                s"$runIdTag ${demultiplexed.project} ${demultiplexed.sampleId} $analysisId reached coverage target with $coverageTotal (minimumWGSCoverage = $minimumWGSCoverage, minimumTargetedCoverage = $minimumTargetedCoverage).")

              for {

                BQSRResult(recalibrated, markDuplicateMetrics) <- BaseQualityScoreRecalibration
                  .bqsr(
                    BQSRInput(allAlignedLanes.map(_.bam).toSet.toStable,
                              indexedReference,
                              knownSites,
                              demultiplexed.project,
                              demultiplexed.sampleId,
                              analysisId))(ResourceConfig.minimal, priorityBam)

                recalibratedPath <- recalibrated.bam.uri.map(_.toString)
                _ = ProgressServer.send(
                  BamAvailable(demultiplexed.project,
                               demultiplexed.sampleId,
                               runIdTag,
                               analysisId,
                               recalibratedPath))

                alignmentQC = intoQCFolder { implicit computationEnvironment =>
                  AlignmentQC.general(
                    AlignmentQCInput(recalibrated, indexedReference))(
                    ResourceConfig.minimal,
                    priorityBam)
                }
                targetSelectionQC = intoQCFolder {
                  implicit computationEnvironment =>
                    AlignmentQC.hybridizationSelection(
                      SelectionQCInput(recalibrated,
                                       indexedReference,
                                       selectionTargetIntervals))(
                      ResourceConfig.collectHSMetrics,
                      priorityBam)
                }
                wgsQC = intoQCFolder { implicit computationEnvironment =>
                  AlignmentQC.wholeGenomeMetrics(
                    CollectWholeGenomeMetricsInput(recalibrated,
                                                   indexedReference))(
                    ResourceConfig.minimal,
                    priorityBam)
                }

                wgsQC <- wgsQC

                targetSelectionQC <- targetSelectionQC
                alignmentQC <- alignmentQC

                wgsMeanCoverage <- AlignmentQC.getWGSMeanCoverage(
                  wgsQC,
                  demultiplexed.project,
                  demultiplexed.sampleId)
                targetedMeanCoverage <- AlignmentQC.getTargetedMeanCoverage(
                  targetSelectionQC,
                  demultiplexed.project,
                  demultiplexed.sampleId
                )

                variantCalls <- variantDiscovery(
                  doVariantCalling = doVariantCalling,
                  wgsMeanCoverage = wgsMeanCoverage,
                  targetedMeanCoverage = targetedMeanCoverage,
                  minimumWGSCoverage = minimumWGSCoverage,
                  minimumTargetedCoverage = minimumTargetedCoverage,
                  bam = recalibrated,
                  indexedReference = indexedReference,
                  contigsFile = contigsFile,
                  dbSnpVcf = dbSnpVcf,
                  project = demultiplexed.project,
                  sampleId = demultiplexed.sampleId,
                  runIdTag = runIdTag,
                  analysisId = analysisId,
                  variantEvaluationIntervals = variantEvaluationIntervals,
                  vqsrTrainingFiles = vqsrTrainingFiles,
                  priorityVcf = priorityVcf,
                  keepVcf = keepVcf.exists(identity)
                )
              } yield
                Some(
                  PerSampleMergedWESResult(
                    bam = recalibrated,
                    haplotypeCallerReferenceCalls =
                      variantCalls.flatMap(_.haplotypeCallerReferenceCalls),
                    gvcf = variantCalls.flatMap(_.genotypedVcf),
                    project = demultiplexed.project,
                    sampleId = demultiplexed.sampleId,
                    alignmentQC = alignmentQC,
                    duplicationQC = markDuplicateMetrics,
                    targetSelectionQC = targetSelectionQC,
                    wgsQC = wgsQC,
                    gvcfQCInterval = variantCalls.map(_.gvcfQCInterval),
                    gvcfQCOverall = variantCalls.map(_.gvcfQCOverall),
                    referenceFasta = indexedReference
                  ))
            }
          } yield
            SingleSamplePipelineResult(
              alignedLanes = alignedLanes,
              mergedRuns = mergedResult,
              coverage = coverageThisRun
            )

    }

  val singleSampleRNA =
    AsyncTask[SingleSamplePipelineInputRNASeq, SingleSamplePipelineResultRNA](
      "__rna-persample-allsamples",
      4) {
      case SingleSamplePipelineInputRNASeq(analysisId,
                                           demultiplexed,
                                           referenceFasta,
                                           geneModelGtf,
                                           readLengths,
                                           qtlToolsArguments,
                                           quantificationGtf,
                                           starVersion) =>
        implicit computationEnvironment =>
          releaseResources

          val projectPriority =
            ResourceConfig.projectPriority(demultiplexed.project)

          val priorityBam = Priority(10000 + projectPriority)
          val priorityPostBam = Priority(20000 + projectPriority)

          val maxReadLength = {
            val readLengthsFromFastQs = demultiplexed.lanes.toSeq
              .flatMap(fqPerLane =>
                fqPerLane.read1.readLength.toSeq ++ fqPerLane.read2.readLength.toSeq)
              .distinct
            if (readLengthsFromFastQs.isEmpty) readLengths.map(_._2).toSeq.max
            else readLengthsFromFastQs.max
          }

          def inSampleFolder[T] =
            appendToFilePrefix[T](
              Seq("projects",
                  demultiplexed.project,
                  demultiplexed.sampleId,
                  analysisId).filter(_.nonEmpty))

          def inReferenceFolder[T] =
            appendToFilePrefix[T](
              Seq("references", analysisId, starVersion.toString)
                .filter(_.nonEmpty))

          for {
            indexedFasta <- inReferenceFolder {
              implicit computationEnvironment =>
                StarAlignment.indexReference(
                  StarIndexInput(referenceFasta, starVersion))(
                  ResourceConfig.createStarIndex)
            }

            processedSample <- inSampleFolder {
              implicit computationEnvironment =>
                for {
                  starResult <- StarAlignment.alignSample(
                    StarAlignmentInput(
                      fastqs = demultiplexed.lanes,
                      project = demultiplexed.project,
                      sampleId = demultiplexed.sampleId,
                      reference = indexedFasta,
                      gtf = geneModelGtf.file,
                      readLength = maxReadLength,
                      starVersion = starVersion
                    ))(ResourceConfig.starAlignment, priorityBam)
                  coordinateSorted <- BWAAlignment.sortByCoordinateAndIndex(
                    starResult.bam.bam)(ResourceConfig.sortBam, priorityBam)
                  counts <- QTLToolsQuantification.quantify(
                    QTLToolsQuantificationInput(
                      coordinateSorted,
                      quantificationGtf,
                      qtlToolsArguments
                    ))(ResourceConfig.qtlToolsQuantification, priorityPostBam)
                  _ <- coordinateSorted.bam.delete

                } yield
                  SingleSamplePipelineResultRNA(analysisId, starResult, counts)
            }

          } yield processedSample

    }

  def parseReadLengthFromRunInfo(
      run: RunfolderReadyForProcessing): Either[String, Map[ReadType, Int]] = {
    run.runFolderPath match {
      case Some(runFolderPath) =>
        val file = new File((runFolderPath: String) + "/RunInfo.xml")
        if (file.canRead) {
          val content = fileutils.openSource(
            new File((runFolderPath: String) + "/RunInfo.xml"))(_.mkString)
          Right(parseReadLengthFromRunInfoXML(content))
        } else Left("Can't read " + file)
      case None =>
        logger.warn("Unimplemented: read length from 3rd party fasqs")
        Right(Map.empty)
    }

  }

  def parseReadLengthFromRunInfoXML(s: String) = {
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

  val countReads = AsyncTask[SharedFile, Long]("__count-reads", 1) {
    case file =>
      implicit computationEnvironment =>
        for {
          file <- file.file
        } yield org.gc.pipelines.util.FastQHelpers.getNumberOfReads(file)
  }

  def liftAlreadyDemultiplexedFastQs(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext): Future[Seq[PerSamplePerRunFastQ]] = {

    val runId = r.runId

    Future.traverse(r.demultiplexedSamples.toSeq.flatten) {
      inputSampleAsFastQ =>
        val fastqs = Future.traverse(inputSampleAsFastQ.lanes.toSeq) {
          inputFastQ =>
            tsc.withFilePrefix(
              Seq("premade_fastqs", inputSampleAsFastQ.project)) {
              implicit tsc =>
                val read1 = new File(inputFastQ.read1Path)
                val read2 = new File(inputFastQ.read2Path)
                val umi: Option[File] = inputFastQ.umi.map { umi =>
                  new File(umi)
                }
                import tasks.util.Uri
                for {
                  read1SF <- SharedFile(Uri("file://" + read1.getAbsolutePath))
                  read2SF <- SharedFile(Uri("file://" + read2.getAbsolutePath))
                  umiSF <- umi match {
                    case None => Future.successful(None)
                    case Some(umif: File) =>
                      SharedFile(umif, umif.getName, false).map(Some(_))
                  }
                  read1Count <- countReads(read1SF)(ResourceConfig.minimal)
                  read2Count <- countReads(read2SF)(ResourceConfig.minimal)
                  umiCount <- umiSF match {
                    case None => Future.successful(None)
                    case Some(umi) =>
                      countReads(umi)(ResourceConfig.minimal).map(Some(_))
                  }
                } yield
                  FastQPerLane(
                    runId,
                    inputFastQ.lane,
                    FastQ(read1SF, read1Count, inputFastQ.read1Length),
                    FastQ(read2SF, read2Count, inputFastQ.read2Length),
                    umiSF.map(umiSF =>
                      FastQ(umiSF, umiCount.get, inputFastQ.umiLength)),
                    PartitionId(0)
                  )

            }
        }

        for {
          fastqs <- fastqs
        } yield
          PerSamplePerRunFastQ(StableSet(fastqs: _*),
                               inputSampleAsFastQ.project,
                               inputSampleAsFastQ.sampleId,
                               runId)

    }
  }

  def executeDemultiplexing(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext)
    : Future[(Seq[PerSamplePerRunFastQ],
              Seq[(DemultiplexingId, DemultiplexingStats.Root)])] =
    r.runFolderPath match {
      case None => Future.successful((Nil, Nil))
      case Some(runFolderPath) =>
        Future
          .traverse(r.runConfiguration.demultiplexingRuns.toSeq) {
            demultiplexingConfig =>
              inDemultiplexingFolder(r.runId,
                                     demultiplexingConfig.demultiplexingId) {
                implicit tsc =>
                  val tenXIndexReadNumber = 0
                  val tenXUmiAssignment =
                    if (demultiplexingConfig.isTenX) Some(tenXIndexReadNumber)
                    else None
                  for {
                    globalIndexSet <- ProtoPipelineStages.fetchGlobalIndexSet(
                      r.runConfiguration)
                    sampleSheet <- ProtoPipelineStages.fetchSampleSheet(
                      demultiplexingConfig.isTenX,
                      demultiplexingConfig.sampleSheet)
                    parsedSampleSheet <- sampleSheet.parse

                    demultiplexed <- Demultiplexing.allLanes(
                      DemultiplexingInput(
                        runFolderPath,
                        sampleSheet,
                        demultiplexingConfig.extraBcl2FastqArguments,
                        globalIndexSet,
                        partitionByLane = demultiplexingConfig.partitionByLane,
                        noPartition = demultiplexingConfig.tenX,
                        partitionByTileCount =
                          demultiplexingConfig.partitionByTileCount,
                        createIndexFastqInReadType =
                          if (demultiplexingConfig.isTenX)
                            Some(ReadType(tenXIndexReadNumber))
                          else None
                      ))(ResourceConfig.minimal,
                         labels = ResourceConfig.projectLabel(
                           parsedSampleSheet.projects: _*))

                    stats <- demultiplexed.stats.get

                    perSampleFastQs = ProtoPipelineStages
                      .groupBySample(
                        demultiplexed.withoutUndetermined,
                        demultiplexingConfig.readAssignment,
                        demultiplexingConfig.umi.orElse(tenXUmiAssignment),
                        r.runId,
                        demultiplexingConfig.isTenX
                      )

                  } yield
                    (perSampleFastQs,
                     demultiplexingConfig.demultiplexingId,
                     stats)

              }
          }
          .map {
            case demultiplexingRuns =>
              val samples = demultiplexingRuns.map(_._1)
              val statsPerDemultiplexingRun =
                demultiplexingRuns.map(d => d._2 -> d._3)
              val flattened = samples.flatten
              val deduplicatedSamples = flattened
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

              (deduplicatedSamples, statsPerDemultiplexingRun)
          }
    }

  private def inDemultiplexingFolder[T](runId: RunId,
                                        demultiplexingId: DemultiplexingId)(
      f: TaskSystemComponents => T)(implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("demultiplexing", runId, demultiplexingId))(f)

  def fetchReferenceFasta(filePath: String, analysisId: AnalysisId)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) = {
    tsc.withFilePrefix(referenceFolder(analysisId)) { implicit tsc =>
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
      case Some(path) => fetchFile(Seq("references"), path).map(Some(_))
    }
  def fetchGenemodel(runConfiguration: RNASeqConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    fetchFileAsReference(runConfiguration.geneModelGtf, runConfiguration)
      .map(GTFFile(_))

  def fetchVariantEvaluationIntervals(runConfiguration: WESConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    fetchFileAsReference(runConfiguration.variantEvaluationIntervals,
                         runConfiguration)
      .map(BedFile(_))

  def fetchDbSnpVcf(runConfiguration: WESConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    for {
      vcf <- fetchFileAsReference(runConfiguration.dbSnpVcf, runConfiguration)
      vcfidx <- fetchFileAsReference(runConfiguration.dbSnpVcf + ".tbi",
                                     runConfiguration)
    } yield VCF(vcf, Some(vcfidx))

  def fetchVqsrTrainingFiles(runConfiguration: WESConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    if (runConfiguration.vqsrMillsAnd1Kg.isDefined)
      (for {
        millsAnd1Kg <- fetchFileAsReference(
          runConfiguration.vqsrMillsAnd1Kg.get,
          runConfiguration)
        millsAnd1KgIdx <- fetchFileAsReference(
          runConfiguration.vqsrMillsAnd1Kg.get + ".tbi",
          runConfiguration)
        oneKg <- fetchFileAsReference(
          runConfiguration.vqsrOneKgHighConfidenceSnps.get,
          runConfiguration)
        oneKgIdx <- fetchFileAsReference(
          runConfiguration.vqsrOneKgHighConfidenceSnps.get + ".tbi",
          runConfiguration)
        hapmap <- fetchFileAsReference(runConfiguration.vqsrHapmap.get,
                                       runConfiguration)
        hapmapIdx <- fetchFileAsReference(
          runConfiguration.vqsrHapmap.get + ".tbi",
          runConfiguration)
        omni <- fetchFileAsReference(runConfiguration.vqsrOneKgOmni.get,
                                     runConfiguration)
        omniIdx <- fetchFileAsReference(
          runConfiguration.vqsrOneKgOmni.get + ".tbi",
          runConfiguration)
        dbSnp138 <- fetchFileAsReference(runConfiguration.vqsrDbSnp138.get,
                                         runConfiguration)
        dbSnp138Idx <- fetchFileAsReference(
          runConfiguration.vqsrDbSnp138.get + ".tbi",
          runConfiguration)
      } yield
        Some(
          VQSRTrainingFiles(
            millsAnd1Kg = VCF(millsAnd1Kg, Some(millsAnd1KgIdx)),
            oneKgHighConfidenceSnps = VCF(oneKg, Some(oneKgIdx)),
            hapmap = VCF(hapmap, Some(hapmapIdx)),
            oneKgOmni = VCF(omni, Some(omniIdx)),
            dbSnp138 = VCF(dbSnp138, Some(dbSnp138Idx))
          ))).recover {
        case e =>
          logger.error(
            "Failed to fetch VQSR training files. Using None. Configuration: " + runConfiguration,
            e)
          None
      } else Future.successful(None)

  def fetchFile(folderName: Seq[String], path: String)(
      implicit tsc: TaskSystemComponents) = {
    tsc.withFilePrefix(folderName) { implicit tsc =>
      val file = new File(path)
      val fileName = file.getName
      logger.debug(s"Fetching $file")
      SharedFile(file, fileName)
    }
  }

  // This replacement is because old data is referring to referencec/ without
  // the analysis id
  private def migrateAnalysisId(analysisId: AnalysisId) =
    if (analysisId == "hg19") "" else analysisId

  private def referenceFolder(analysisId: AnalysisId): Seq[String] =
    Seq("references", migrateAnalysisId(analysisId)).filter(_.nonEmpty)

  def fetchFileAsReference(path: String, runConfiguration: WESConfiguration)(
      implicit tsc: TaskSystemComponents) =
    fetchFile(referenceFolder(runConfiguration.analysisId), path)
  def fetchFileAsReference(path: String, runConfiguration: RNASeqConfiguration)(
      implicit tsc: TaskSystemComponents) =
    fetchFile(referenceFolder(runConfiguration.analysisId), path)

  def fetchTargetIntervals(runConfiguration: WESConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) = {
    tsc.withFilePrefix(referenceFolder(runConfiguration.analysisId)) {
      implicit tsc =>
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

  def fetchContigsFile(runConfiguration: WESConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) = {
    tsc.withFilePrefix(Seq("references")) { implicit tsc =>
      runConfiguration.variantCallingContigs match {
        case None => Future.successful(None)
        case Some(path) =>
          val file = new File(path)
          val fileName = file.getName
          logger.debug(s"Fetching variant calling contigs file $file")
          SharedFile(file, fileName).map(f => Some(ContigsFile(f))).andThen {
            case Success(_) =>
              logger.debug(s"Fetched target contigs for variant calling")
            case Failure(e) =>
              logger.error(s"Failed to fetch variant calling contigs $file", e)

          }
      }

    }
  }
  def fetchKnownSitesFiles(runConfiguration: WESConfiguration)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) = {
    val files =
      runConfiguration.bqsrKnownSites.toSeq

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
    *
    * @param isTenX if true the sample name is transformed and the part after the last _ is dropped
    *                this is because the TenX#resolve produces a sample sheet where the original
    *                sample id is concatenated with _{index}
    */
  def groupBySample(demultiplexed: DemultiplexedReadData,
                    readAssignment: (Int, Int),
                    umi: Option[Int],
                    runId: RunId,
                    isTenX: Boolean): Seq[PerSamplePerRunFastQ] = {
    val groupedBySamples = demultiplexed.fastqs.toSeq
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
    if (!isTenX) groupedBySamples
    else {
      groupedBySamples
        .map { perSamplePerRunFastQ =>
          val sampleId = perSamplePerRunFastQ.sampleId
          val originalSampleId = sampleId
            .split("_")
            .dropRight(1)
            .mkString("_")
          (originalSampleId, perSamplePerRunFastQ)
        }
        .groupBy(_._1)
        .toSeq
        .map {
          case (originalSampleId, group) =>
            group.head._2.copy(
              sampleId = SampleId(originalSampleId),
              lanes = group.flatMap(_._2.lanes.toSeq).toSet.toStable
            )
        }
    }
  }

  def selectReadType(fqs: Seq[FastQWithSampleMetadata], readType: ReadType) =
    fqs
      .filter(_.readType == readType)
      .headOption
      .map(_.fastq)

}
