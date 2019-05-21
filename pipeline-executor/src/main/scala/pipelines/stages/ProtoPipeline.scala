package org.gc.pipelines.stages

import scala.concurrent.{ExecutionContext, Future}
import tasks._
import org.gc.pipelines.application.{
  Pipeline,
  RunfolderReadyForProcessing,
  WESConfiguration,
  RNASeqConfiguration,
  TenXConfiguration,
  AnalysisAssignments,
  ProgressData
}
import org.gc.pipelines.model._
import org.gc.pipelines.application.{SendProgressData}
import org.gc.pipelines.application.ProgressData._
import org.gc.pipelines.util.{ResourceConfig, traverseAll}
import org.gc.pipelines.util.StableSet.syntax
import org.gc.pipelines.util.StableSet
import com.typesafe.scalalogging.StrictLogging
import scala.util.{Success, Failure}

/* Implementation of `Pipeline`
 *
 * This class is the root of all the bioinformatic processing steps
 */
class ProtoPipeline(progressServer: SendProgressData)(
    implicit EC: ExecutionContext)
    extends Pipeline[PerSamplePerRunFastQ, SampleResult, DeliverableList]
    with StrictLogging {

  def getKeysOfDemultiplexedSample(
      d: PerSamplePerRunFastQ): (Project, SampleId, RunId) =
    (d.project, d.sampleId, d.runId)

  def getKeysOfSampleResult(d: SampleResult): (Project, SampleId, RunId) =
    (d.project, d.sampleId, d.lastRunId)

  def processCompletedRun(samples: Seq[SampleResult])(
      implicit tsc: TaskSystemComponents): Future[(RunId, Boolean)] = {
    require(samples.map(_.lastRunId).distinct.size == 1,
            s"Multiple run ids found: ${samples.map(_.lastRunId)}")
    val runId = samples.head.lastRunId

    val fastqsOfThisRun =
      samples
        .flatMap(_.demultiplexed)
        .filter(_.runId == runId)
        .map(_.withoutRunId)

    val sampleQCsWES =
      samples.flatMap(_.extractWESQCFiles)

    inRunQCFolder(runId) { implicit tsc =>
      for {

        _ <- AlignmentQC.runQCTable(
          RunQCTableInput(
            runId + "." + samples.size,
            sampleQCsWES.toSet.toStable,
            samples
              .flatMap(sampleResult =>
                sampleResult.rna.toSeq.map(rnaResult =>
                  (rnaResult.analysisId, rnaResult.star)))
              .toSet
              .toStable
          ))(ResourceConfig.minimal)

        _ <- ReadQC.readQC(
          ReadQCInput(fastqsOfThisRun.toSet.toStable,
                      runId + "." + samples.size))(ResourceConfig.minimal)
      } yield (runId, true)
    }

  }
  def processCompletedRuns(samples: Map[RunId, Seq[SampleResult]])(
      implicit tsc: TaskSystemComponents): Future[Unit] = {

    val sampleQCsWES =
      samples.toSeq.flatMap(_._2.flatMap(_.extractWESQCFiles))
    val sampleQCsRNA =
      samples.toSeq.flatMap(_._2.flatMap(_.rna.toSeq.map(rnaResult =>
        (rnaResult.analysisId, rnaResult.star))))

    val allSamples = samples.toSeq.flatMap(_._2.map(_.sampleId)).distinct
    val allRuns = samples.toSeq.map(_._1).distinct

    val individualRunQCs = Future.traverse(samples.toSeq.map(_._2)) { samples =>
      processCompletedRun(samples)
    }
    for {
      _ <- inAllQCFolder { implicit tsc =>
        AlignmentQC.runQCTable(
          RunQCTableInput(
            s"s${allSamples.size}.r${allRuns.size}.hs${allSamples.hashCode}.hr${allRuns.hashCode}",
            sampleQCsWES.toSet.toStable,
            sampleQCsRNA.toSet.toStable
          ))(ResourceConfig.minimal)

      }
      _ <- individualRunQCs
    } yield ()

  }

  def processCompletedProject(samples0: Seq[SampleResult])(
      implicit tsc: TaskSystemComponents)
    : Future[(Project, Boolean, Option[DeliverableList])] = {
    require(samples0.map(_.project).distinct.size == 1, samples0.toString)
    val project = samples0.head.project

    // See Migration0001.scala why this is here
    val samples =
      samples0.map { sample =>
        val existHg19 = sample.wes.exists {
          case (_, analysisConfig, _) => analysisConfig.analysisId == "hg19"
        }
        if (existHg19)
          sample.copy(wes = sample.wes.filterNot {
            case (_, analysisConfig, _) => analysisConfig.analysisId == ""
          })
        else sample

      }

    val fastqsOfThisRun =
      samples
        .flatMap(_.demultiplexed)
        .filter(_.project == project)
        .map(_.withoutRunId)

    val runsIncluded = samples0.flatMap(_.runFolders.map(_.runId)).toSet

    val sampleQCsWES =
      samples.flatMap(_.extractWESQCFiles)

    val wesResultsByAnalysisId
      : Seq[(AnalysisId,
             Seq[(PerSampleMergedWESResult, SingleSampleConfiguration)])] =
      samples
        .flatMap { sampleResult =>
          sampleResult.wes
            .flatMap {
              case (wesResult, wesConfig, _) =>
                wesResult.mergedRuns.map { result =>
                  (result, wesConfig)
                }
            }
            .map {
              case (wesResult, wesConfig) =>
                val analysisId =
                  // this replacement is due to the migration which introduced analysis ids
                  // analyses without an id were given the empty string, but semantically they are
                  // equivalent to the hg19
                  if (wesConfig.analysisId == "") AnalysisId("hg19")
                  else wesConfig.analysisId

                (analysisId, (wesResult, wesConfig))
            }
        }
        .groupBy(_._1)
        .toSeq
        .map { case (analysisId, group) => (analysisId, group.map(_._2)) }

    val startProjectQC = inProjectQCFolder(project) { implicit tsc =>
      for {

        qctables <- AlignmentQC.runQCTable(
          RunQCTableInput(
            project + "." + samples.size,
            sampleQCsWES.toSet.toStable,
            samples
              .flatMap(_.rna.toSeq.map(rnaResult =>
                (rnaResult.analysisId, rnaResult.star)))
              .toSet
              .toStable
          ))(ResourceConfig.minimal)

        reads <- ReadQC.readQC(ReadQCInput(
          fastqsOfThisRun.toSet.toStable,
          project + ".s" + samples.size + "r" + runsIncluded.size + ".h" + runsIncluded.hashCode))(
          ResourceConfig.minimal)

      } yield (qctables, reads)
    }

    def assertUniqueAndGet[T](s: Seq[T]) =
      if (s.distinct.size == 1) Right(s.head)
      else {
        Left(s"Unicity failed on configuration settings of $project $s")
      }

    val startJointCalls =
      Future
        .traverse(wesResultsByAnalysisId) {
          case (analysisId, wesResults) =>
            val configuration = for {
              indexedReference <- assertUniqueAndGet(
                wesResults.map(_._1.referenceFasta))
              dbSnpVcf <- assertUniqueAndGet(wesResults.map(_._2.dbSnpVcf))
              vqsrTrainingFiles <- assertUniqueAndGet(
                wesResults.map(_._2.vqsrTrainingFiles))
              jointCall <- assertUniqueAndGet(
                wesResults.map(
                  _._2.wesConfiguration.doJointCalls.getOrElse(false)
                )
              )
              mergeSingleCalls <- assertUniqueAndGet(
                wesResults.map(
                  _._2.wesConfiguration.mergeSingleCalls.getOrElse(false)
                )
              )
              contigs <- assertUniqueAndGet(
                wesResults.map(_._2.variantCallingContigs))
            } yield
              (indexedReference,
               dbSnpVcf,
               vqsrTrainingFiles,
               jointCall,
               contigs,
               mergeSingleCalls)

            configuration match {
              case Left(error) =>
                logger.error(
                  "Skip joint call due to configuration error: " + error)
                Future.successful((None, None))
              case Right(
                  (indexedReference,
                   dbSnpVcf,
                   vqsrTrainingFiles,
                   jointCall,
                   contigs,
                   mergeSingleCalls)) => {
                def jointCallInputVCFs =
                  wesResults
                    .flatMap(_._1.haplotypeCallerReferenceCalls.toSeq)
                    .toSet

                def singleCallVCFs =
                  wesResults
                    .flatMap(_._1.gvcf.toSeq)
                    .toSet

                val jointCalls =
                  if (jointCall && jointCallInputVCFs.nonEmpty) {
                    progressServer.send(
                      ProgressData.JointCallsStarted(
                        project,
                        analysisId,
                        samples.map(_.sampleId).toSet,
                        samples.flatMap(_.runFolders.map(_.runId)).toSet))
                    inProjectJointCallFolder(project, analysisId) {
                      implicit tsc =>
                        HaplotypeCaller
                          .jointCall(
                            JointCallInput(
                              jointCallInputVCFs.toStable,
                              indexedReference,
                              dbSnpVcf,
                              vqsrTrainingFiles,
                              project + "." + analysisId,
                              contigs
                            ))(ResourceConfig.minimal)
                          .map(Some(_))
                          .andThen({
                            case Success(Some(vcf)) =>
                              for {
                                vcfPath <- vcf.vcf.uri
                              } progressServer.send(
                                ProgressData.JointCallsAvailable(
                                  project,
                                  analysisId,
                                  samples.map(_.sampleId).toSet,
                                  samples
                                    .flatMap(_.runFolders.map(_.runId))
                                    .toSet,
                                  vcfPath.toString))

                          })
                    }
                  } else Future.successful(None)

                val mergedCalls =
                  if (mergeSingleCalls && jointCallInputVCFs.nonEmpty) {
                    progressServer.send(
                      ProgressData.JointCallsStarted(
                        project,
                        analysisId,
                        samples.map(_.sampleId).toSet,
                        samples.flatMap(_.runFolders.map(_.runId)).toSet))
                    inProjectMergedSingleCallFolder(project, analysisId) {
                      implicit tsc =>
                        HaplotypeCaller
                          .mergeSingleCalls(
                            MergeSingleCallsInput(
                              jointCallInputVCFs.toStable,
                              singleCallVCFs.toStable,
                              indexedReference,
                              dbSnpVcf,
                              project + "." + analysisId,
                              contigs
                            ))(ResourceConfig.minimal)
                          .map(Some(_))
                          .andThen({
                            case Success(Some(vcf)) =>
                              for {
                                vcfPath <- vcf.vcf.uri
                              } progressServer.send(
                                ProgressData.JointCallsAvailable(
                                  project,
                                  analysisId,
                                  samples.map(_.sampleId).toSet,
                                  samples
                                    .flatMap(_.runFolders.map(_.runId))
                                    .toSet,
                                  vcfPath.toString))

                          })
                    }
                  } else Future.successful(None)

                for {
                  jointCalls <- jointCalls
                  mergedCalls <- mergedCalls
                } yield (jointCalls, mergedCalls)
              }
            }
        }

    for {
      (qcTables, reads) <- startProjectQC
      jointAndMergedCalls <- startJointCalls
      deliverables <- inDeliverablesFolder { implicit tsc =>
        val jointAndMergedCallVcfFileSet = jointAndMergedCalls.flatMap {
          case (joint, merged) =>
            joint.toSeq.map(vcfWithIndex => project -> vcfWithIndex.vcf) ++
              merged.toSeq.map(vcfWithIndex => project -> vcfWithIndex.vcf)

        }.toSet

        val files =
          (jointAndMergedCallVcfFileSet ++ Set(project -> qcTables.htmlTable,
                                               project -> qcTables.rnaCsvTable,
                                               project -> reads.plots)).toStable

        Delivery.collectDeliverables(
          CollectDeliverablesInput(samples.toSet.toStable, files))(
          ResourceConfig.minimal)
      }
    } yield {
      deliverables.lists.foreach {
        case (_, _, deliveryListAvailable) =>
          progressServer.send(
            deliveryListAvailable
          )
      }
      (project, true, Some(deliverables))
    }
  }

  def sendFastQPathsToProgressServer(
      runId: RunId,
      samples: Seq[PerSamplePerRunFastQ],
      stats: Seq[(DemultiplexingId, DemultiplexingStats.Root)])(
      implicit tsc: TaskSystemComponents): Unit = {
    val samplesWithFastqPaths = Future.traverse(samples) {
      perSamplePerRunFastq =>
        val files = perSamplePerRunFastq.lanes.toSeq.flatMap(
          lane =>
            List(lane.read1.file, lane.read2.file) ++ lane.umi.toList
              .map(_.file))

        for {
          paths <- Future.traverse(files)(_.uri)
        } yield {
          (perSamplePerRunFastq.runId,
           perSamplePerRunFastq.project,
           perSamplePerRunFastq.sampleId,
           paths.map(_.toString))
        }
    }

    samplesWithFastqPaths.andThen {
      case Success(samplesWithFastqPaths) =>
        progressServer.send(Demultiplexed(runId, samplesWithFastqPaths.map {
          case (_, project, sampleId, paths) =>
            (project, sampleId, paths.toSet)
        }, stats))
      case Failure(e) =>
        logger.error("Failed to fetch paths for demultiplexed fastqs", e)
    }
  }

  def demultiplex(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents): Future[Seq[PerSamplePerRunFastQ]] = {
    progressServer.send(DemultiplexStarted(r.runId))
    (r.runFolderPath match {
      case Some(_) => ProtoPipelineStages.executeDemultiplexing(r)
      case None =>
        ProtoPipelineStages.liftAlreadyDemultiplexedFastQs(r).map { samples =>
          (samples, Nil)
        }
    }).andThen {
        case Success((samples, stats)) =>
          sendFastQPathsToProgressServer(r.runId, samples, stats)
        case Failure(_) =>
          progressServer.send(DemultiplexFailed(r.runId))
      }
      .map {
        case (samples, _) => samples
      }
  }

  /* Entry point of per sample processing */
  def processSample(r: RunfolderReadyForProcessing,
                    analysisAssignments: AnalysisAssignments,
                    pastSampleResult: Option[SampleResult],
                    demultiplexedSample: PerSamplePerRunFastQ)(
      implicit tsc: TaskSystemComponents): Future[Option[SampleResult]] = {

    if (demultiplexedSample.lanes.toSeq.isEmpty) {
      logger.info(s"Skipping because no fastq in $demultiplexedSample.")
      Future.successful(None)
    } else {

      val lastRun =
        r.runConfiguration.isLastRunOfSample(demultiplexedSample.project,
                                             demultiplexedSample.sampleId)

      def modifyConfigurationForLastRun(conf: WESConfiguration) =
        if (lastRun)
          conf.ignoreMinimumCoverage
        else conf

      progressServer.send(
        SampleProcessingStarted(demultiplexedSample.project,
                                demultiplexedSample.sampleId,
                                r.runId))

      ProtoPipelineStages.parseReadLengthFromRunInfo(r) match {
        case Left(error) =>
          logger.error(s"$error")
          progressServer.send(
            SampleProcessingFailed(demultiplexedSample.project,
                                   demultiplexedSample.sampleId,
                                   r.runId))
          Future.successful(None)
        case Right(readLengths) =>
          val fastpReport = startFastpReports(demultiplexedSample)

          val selectedWESConfigurations = analysisAssignments.assignments
            .get(demultiplexedSample.project)
            .getOrElse(Nil)
            .collect {
              case conf: WESConfiguration => modifyConfigurationForLastRun(conf)
            }

          val selectedRNASeqConfigurations = analysisAssignments.assignments
            .get(demultiplexedSample.project)
            .getOrElse(Nil)
            .collect {
              case conf: RNASeqConfiguration => conf
            }

          val selected10XSeqConfigurations = analysisAssignments.assignments
            .get(demultiplexedSample.project)
            .getOrElse(Nil)
            .collect {
              case conf: TenXConfiguration => conf
            }

          logger.info(s"Processing sample ${demultiplexedSample.project} ${demultiplexedSample.sampleId} from last run ${demultiplexedSample.runId} in analyses: WES: ${selectedWESConfigurations
            .map(_.analysisId)}, RNA: ${selectedRNASeqConfigurations.map(
            _.analysisId)}, 10X: ${selected10XSeqConfigurations.map(_.analysisId)}")

          val perSampleResultsWES =
            traverseAll(selectedWESConfigurations) { conf =>
              logger.info(
                demultiplexedSample.runId + " " + demultiplexedSample.project + " " + demultiplexedSample.sampleId + " past result: " + pastSampleResult
                  .map(_.runFolders.map(_.runId)))

              val pastResultsMatchingAnalysisId
                : Option[(SingleSamplePipelineResult,
                          SingleSampleConfiguration,
                          List[(RunId, MeanCoverageResult)])] = pastSampleResult
                .flatMap(_.wes
                  .find {
                    case (_, wesConfigurationOfPastSample, _) =>
                      val matchingAnalysisId = wesConfigurationOfPastSample.analysisId == conf.analysisId
                      val matchingMigratedOldAnalysisId = wesConfigurationOfPastSample.analysisId == "" && conf.analysisId == "hg19"

                      logger.debug(
                        "matchingAnalysisId: " + matchingAnalysisId + " matchingMigratedOldAnalysisId: " + matchingMigratedOldAnalysisId + " " + demultiplexedSample + " " + conf)

                      matchingAnalysisId || matchingMigratedOldAnalysisId
                  })

              val pastAlignedLanes =
                pastResultsMatchingAnalysisId.toSeq.flatMap {
                  case (wesResultOfPastSample, _, _) =>
                    wesResultOfPastSample.alignedLanes.toSeq
                }

              val pastPerRunCoverages = pastResultsMatchingAnalysisId
                .map { case (_, _, coverages) => coverages }
                .getOrElse(Nil)

              wes(
                demultiplexedSample,
                conf,
                pastAlignedLanes.toSet.toStable
              ).map {
                case (result, config) =>
                  (result,
                   config,
                   (demultiplexedSample.runId, result.coverage) :: pastPerRunCoverages)
              }
            }

          val perSampleResultsRNA = if (readLengths.isEmpty) {
            logger.warn(
              s"Empty read lengths ${demultiplexedSample.project} ${demultiplexedSample.sampleId} ${demultiplexedSample.runId}. RNASeq analysis on 3rd party fastqs not implemented. A rough read length estimate is needed by STAR.")
            Future.successful(Nil)

          } else {
            val currentDemultiplexedFastQWithPreviousFastQs =
              demultiplexedSample.copy(
                lanes =
                  (demultiplexedSample.lanes.toSeq ++ pastSampleResult.toSeq
                    .flatMap(_.demultiplexed.flatMap(_.lanes.toSeq))).toSet.toStable
              )
            traverseAll(selectedRNASeqConfigurations)(
              rna(currentDemultiplexedFastQWithPreviousFastQs, _, readLengths)
            )
          }

          val perSampleResults10X =
            traverseAll(selected10XSeqConfigurations)(_ =>
              tenX(demultiplexedSample))

          for {

            perSampleResultsWES <- perSampleResultsWES
            perSampleResultsRNA <- perSampleResultsRNA
            perSampleResults10X <- perSampleResults10X

            fastpReport <- fastpReport

          } yield {
            val pastFastpReports =
              pastSampleResult.toSeq.flatMap(_.fastpReports)
            val pastDemultiplexed =
              pastSampleResult.toSeq.flatMap(_.demultiplexed)
            val pastRunFolders = pastSampleResult.toSeq.flatMap(_.runFolders)
            val pastRNASeqResults = pastSampleResult.toSeq.flatMap(_.rna)
            val past10XResults = pastSampleResult.toSeq.flatMap(_.tenX)

            progressServer.send(
              SampleProcessingFinished(demultiplexedSample.project,
                                       demultiplexedSample.sampleId,
                                       demultiplexedSample.runId))

            Some(
              SampleResult(
                wes = perSampleResultsWES,
                rna = pastRNASeqResults ++ perSampleResultsRNA,
                tenX = past10XResults ++ perSampleResults10X,
                demultiplexed = pastDemultiplexed :+ demultiplexedSample,
                fastpReports = pastFastpReports :+ fastpReport,
                runFolders = pastRunFolders :+ r,
                sampleId = demultiplexedSample.sampleId,
                project = demultiplexedSample.project
              )
            )
          }

      }
    }
  }

  private def wes(
      sampleForWESAnalysis: PerSamplePerRunFastQ,
      conf: WESConfiguration,
      previouslyAlignedLanes: StableSet[BamWithSampleMetadataPerLane])(
      implicit tsc: TaskSystemComponents) =
    for {
      reference <- ProtoPipelineStages.fetchReferenceFasta(conf.referenceFasta,
                                                           conf.analysisId)
      knownSites <- ProtoPipelineStages.fetchKnownSitesFiles(conf)

      selectionTargetIntervals <- ProtoPipelineStages.fetchTargetIntervals(conf)
      contigsFile <- ProtoPipelineStages.fetchContigsFile(conf)
      dbSnpVcf <- ProtoPipelineStages.fetchDbSnpVcf(conf)
      variantEvaluationIntervals <- ProtoPipelineStages
        .fetchVariantEvaluationIntervals(conf)
      vqsrTrainingFiles <- ProtoPipelineStages.fetchVqsrTrainingFiles(conf)
      perSampleResultsWES <- {
        logger.debug(
          s"Start main wes task of ${sampleForWESAnalysis.project} ${sampleForWESAnalysis.sampleId}")
        // at this point the job is sent to the tasks library
        // further execution will happen on a worker node
        // results will be checkpointed
        ProtoPipelineStages.singleSampleWES(
          SingleSamplePipelineInput(
            conf.analysisId,
            sampleForWESAnalysis.withoutRunId.withoutReadLength,
            reference,
            knownSites.toSet.toStable,
            selectionTargetIntervals,
            dbSnpVcf,
            variantEvaluationIntervals,
            previouslyAlignedLanes,
            !conf.doVariantCalls.exists(_ == false),
            minimumWGSCoverage = conf.minimumWGSCoverage,
            minimumTargetCoverage = conf.minimumTargetCoverage,
            contigsFile = contigsFile,
            vqsrTrainingFiles =
              if (conf.singleSampleVqsr.exists(_ == true)) vqsrTrainingFiles
              else None,
            keepVcf = conf.keepVcf
          ))(
          ResourceConfig.minimal,
          labels =
            ResourceConfig.projectAndSampleLabel(sampleForWESAnalysis.project,
                                                 sampleForWESAnalysis.sampleId,
                                                 conf.analysisId,
                                                 sampleForWESAnalysis.runId)
        )
      }
      _ <- sendUpdatesProgressServer(perSampleResultsWES,
                                     sampleForWESAnalysis.runId,
                                     sampleForWESAnalysis.project,
                                     sampleForWESAnalysis.sampleId,
                                     conf.analysisId)

    } yield
      (perSampleResultsWES,
       SingleSampleConfiguration(conf.analysisId,
                                 dbSnpVcf,
                                 vqsrTrainingFiles,
                                 conf,
                                 contigsFile))

  private def sendUpdatesProgressServer(
      wesResult: SingleSamplePipelineResult,
      runId: RunId,
      project: Project,
      sampleId: SampleId,
      analysisId: AnalysisId)(implicit tsc: TaskSystemComponents) = {
    progressServer.send(
      FastCoverageAvailable(project,
                            sampleId,
                            runId,
                            analysisId,
                            wesResult.coverage.all))
    wesResult.mergedRuns match {
      case None => Future.successful(())
      case Some(mergedResults) =>
        for {
          bamPath <- mergedResults.bam.bam.uri.map(_.toString)
          _ = progressServer.send(
            BamAvailable(mergedResults.project,
                         mergedResults.sampleId,
                         runId.toString,
                         analysisId,
                         bamPath))
          _ <- mergedResults.gvcf match {
            case Some(vcf) =>
              vcf.vcf.uri.map(_.toString).map { vcfPath =>
                progressServer.send(
                  VCFAvailable(project,
                               sampleId,
                               runId.toString,
                               analysisId,
                               vcfPath))
              }
            case _ => Future.successful(())
          }

        } yield ()
    }

  }

  private def rna(
      samplesForRNASeqAnalysis: PerSamplePerRunFastQ,
      conf: RNASeqConfiguration,
      readLengths: Map[ReadType, Int])(implicit tsc: TaskSystemComponents) =
    for {
      gtf <- ProtoPipelineStages.fetchGenemodel(conf)
      quantificationGtf <- ProtoPipelineStages
        .fetchFileAsReference(conf.quantificationGtf, conf)
        .map(GTFFile(_))
      reference <- ProtoPipelineStages.fetchReferenceFasta(conf.referenceFasta,
                                                           conf.analysisId)
      perSampleResultsRNA <- ProtoPipelineStages.singleSampleRNA(
        SingleSamplePipelineInputRNASeq(
          conf.analysisId,
          samplesForRNASeqAnalysis.withoutRunId,
          reference,
          gtf,
          readLengths.toSeq.toSet.toStable,
          conf.qtlToolsCommandLineArguments,
          quantificationGtf = quantificationGtf,
          starVersion = conf.starVersion match {
            case Some("2.6.1c") => StarVersion.Star261c
            case _              => StarVersion.Star261a
          }
        ))(ResourceConfig.minimal,
           labels =
             ResourceConfig.projectLabel(samplesForRNASeqAnalysis.project))

      bamPath <- perSampleResultsRNA.star.bam.bam.file.uri.map(_.toString)
      _ = progressServer.send(
        BamAvailable(samplesForRNASeqAnalysis.project,
                     samplesForRNASeqAnalysis.sampleId,
                     samplesForRNASeqAnalysis.runId.toString,
                     conf.analysisId,
                     bamPath))

    } yield perSampleResultsRNA

  private def tenX(sampleFor10XAnalysis: PerSamplePerRunFastQ)(
      implicit tsc: TaskSystemComponents) = {

    for {

      concatenated <- inProject10XFolder(sampleFor10XAnalysis.project) {
        implicit tsc =>
          TenXStages.concatenateFastQ(sampleFor10XAnalysis.withoutRunId)(
            ResourceConfig.minimal,
            labels = ResourceConfig.projectLabel(sampleFor10XAnalysis.project))
      }

    } yield concatenated
  }

  private def startFastpReports(perSampleFastQs: PerSamplePerRunFastQ)(
      implicit tsc: TaskSystemComponents): Future[FastpReport] =
    tsc.withFilePrefix(
      Seq("projects",
          perSampleFastQs.project,
          perSampleFastQs.sampleId,
          "fastp",
          perSampleFastQs.runId)) { implicit tsc =>
      Fastp.report(perSampleFastQs)(ResourceConfig.fastp(perSampleFastQs))
    }

  private def inRunQCFolder[T](runId: RunId)(f: TaskSystemComponents => T)(
      implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("runQC", runId))(f)

  private def inAllQCFolder[T](f: TaskSystemComponents => T)(
      implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("allQC"))(f)

  private def inProject10XFolder[T](project: Project)(
      f: TaskSystemComponents => T)(implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("projects", project, "tenxfastqs"))(f)

  private def inProjectQCFolder[T](project: Project)(
      f: TaskSystemComponents => T)(implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("projectQC", project))(f)

  private def inProjectJointCallFolder[T](project: Project,
                                          analysisId: AnalysisId)(
      f: TaskSystemComponents => T)(implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("projects", project, "joint-calls", analysisId))(f)

  private def inProjectMergedSingleCallFolder[T](project: Project,
                                                 analysisId: AnalysisId)(
      f: TaskSystemComponents => T)(implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(
      Seq("projects", project, "merged-single-calls", analysisId))(f)

  private def inDeliverablesFolder[T](f: TaskSystemComponents => T)(
      implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("deliverables"))(f)

}
