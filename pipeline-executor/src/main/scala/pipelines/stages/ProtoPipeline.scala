package org.gc.pipelines.stages

import scala.concurrent.{ExecutionContext, Future}
import tasks._
import org.gc.pipelines.application.{
  Pipeline,
  RunfolderReadyForProcessing,
  WESConfiguration,
  RNASeqConfiguration,
  AnalysisAssignments
}
import org.gc.pipelines.model._
import org.gc.pipelines.application.{SendProgressData}
import org.gc.pipelines.application.ProgressData._
import org.gc.pipelines.util.{ResourceConfig, traverseAll}
import org.gc.pipelines.util.StableSet.syntax
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

  def canProcess(r: RunfolderReadyForProcessing) = true

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

  def processCompletedProject(samples0: Seq[SampleResult])(
      implicit tsc: TaskSystemComponents)
    : Future[(Project, Boolean, Option[DeliverableList])] = {
    require(samples0.map(_.project).distinct.size == 1, samples0.toString)
    val project = samples0.head.project

    // See Migration0001.scala why this is here
    val samples =
      samples0.map { sample =>
        val existHg19 = sample.wes.exists {
          case (_, analysisConfig) => analysisConfig.analysisId == "hg19"
        }
        if (existHg19)
          sample.copy(wes = sample.wes.filterNot {
            case (_, analysisConfig) => analysisConfig.analysisId == ""
          })
        else sample

      }

    val fastqsOfThisRun =
      samples
        .flatMap(_.demultiplexed)
        .filter(_.project == project)
        .map(_.withoutRunId)

    val sampleQCsWES =
      samples.flatMap(_.extractWESQCFiles)

    val wesResultsByAnalysisId
      : Seq[(AnalysisId,
             Seq[(SingleSamplePipelineResult, SingleSampleConfiguration)])] =
      samples
        .flatMap { sampleResult =>
          sampleResult.wes.map {
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

    def projectQC = inProjectQCFolder(project) { implicit tsc =>
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

        reads <- ReadQC.readQC(
          ReadQCInput(fastqsOfThisRun.toSet.toStable,
                      project + "." + samples.size))(ResourceConfig.minimal)

      } yield (qctables, reads)
    }

    def assertUniqueAndGet[T](s: Seq[T]) =
      if (s.distinct.size == 1) Right(s.head)
      else {
        Left(s"Unicity failed on configuration settings of $project $s")
      }

    def jointCalls =
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
              contigs <- assertUniqueAndGet(
                wesResults.map(_._2.variantCallingContigs))
            } yield
              (indexedReference,
               dbSnpVcf,
               vqsrTrainingFiles,
               jointCall,
               contigs)

            configuration match {
              case Left(error) =>
                logger.error(
                  "Skip joint call due to configuration error: " + error)
                Future.successful(None)
              case Right(
                  (indexedReference,
                   dbSnpVcf,
                   vqsrTrainingFiles,
                   jointCall,
                   contigs)) =>
                inProjectJointCallFolder(project, analysisId) { implicit tsc =>
                  def jointCallInputVCFs =
                    wesResults
                      .filter(_._2.wesConfiguration.doJointCalls
                        .getOrElse(false))
                      .flatMap(_._1.haplotypeCallerReferenceCalls.toSeq)
                      .toSet

                  if (jointCall && jointCallInputVCFs.nonEmpty)
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
                  else Future.successful(None)
                }
            }
        }
        .map(_.collect { case Some(calls) => calls })

    for {
      (qcTables, reads) <- projectQC
      jointCallsVCF <- jointCalls
      deliverables <- inDeliverablesFolder { implicit tsc =>
        val jointCallVcfFileSet = jointCallsVCF
          .map(vcfWithIndex => project -> vcfWithIndex.vcf)
          .toSet

        val files =
          (jointCallVcfFileSet ++ Set(project -> qcTables.htmlTable,
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

  def sendFastQPathsToProgressServer(runId: RunId,
                                     samples: Seq[PerSamplePerRunFastQ])(
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
        }))
      case Failure(e) =>
        logger.error("Failed to fetch paths for demultiplexed fastqs", e)
    }
  }

  def demultiplex(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents): Future[Seq[PerSamplePerRunFastQ]] = {
    progressServer.send(DemultiplexStarted(r.runId))
    (r.runFolderPath match {
      case Some(_) => ProtoPipelineStages.executeDemultiplexing(r)
      case None    => ProtoPipelineStages.liftAlreadyDemultiplexedFastQs(r)
    }).andThen {
      case Success(samples) =>
        sendFastQPathsToProgressServer(r.runId, samples)
      case Failure(_) =>
        progressServer.send(DemultiplexFailed(r.runId))
    }
  }

  /* Entry point of per sample processing */
  def processSample(r: RunfolderReadyForProcessing,
                    analysisAssignments: AnalysisAssignments,
                    pastSampleResult: Option[SampleResult],
                    demultiplexedSample: PerSamplePerRunFastQ)(
      implicit tsc: TaskSystemComponents): Future[Option[SampleResult]] = {

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
        logger.info(s"${r.runId} read lengths: ${readLengths.mkString(", ")}")

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

        val perSampleResultsWES =
          traverseAll(selectedWESConfigurations) { conf =>
            logger.info(
              demultiplexedSample.runId + " " + demultiplexedSample.project + " " + demultiplexedSample.sampleId + " past result: " + pastSampleResult
                .map(_.runFolders.map(_.runId)))

            val matchingPastResults = pastSampleResult
              .flatMap(_.wes
                .find {
                  case (_, wesConfigurationOfPastSample) =>
                    val matchingAnalysisId = wesConfigurationOfPastSample.analysisId == conf.analysisId
                    val matchingMigratedOldAnalysisId = wesConfigurationOfPastSample.analysisId == "" && conf.analysisId == "hg19"

                    logger.debug(
                      "matchingAnalysisId: " + matchingAnalysisId + " matchingMigratedOldAnalysisId: " + matchingMigratedOldAnalysisId + " " + demultiplexedSample + " " + conf)

                    matchingAnalysisId || matchingMigratedOldAnalysisId
                }
                .map {
                  case (wesResultOfPastSample, _) =>
                    wesResultOfPastSample.uncalibrated
                })

            wes(
              demultiplexedSample,
              conf,
              matchingPastResults
            )
          }

        val perSampleResultsRNA = if (readLengths.isEmpty) {
          logger.warn(
            "Empty read lengths. RNASeq analysis on 3rd party fastqs not implemented. A rough read length estimate is needed by STAR.")
          Future.successful(Nil)

        } else
          traverseAll(selectedRNASeqConfigurations)(
            rna(demultiplexedSample, _, readLengths))

        for {

          perSampleResultsWES <- perSampleResultsWES
          perSampleResultsRNA <- perSampleResultsRNA

          fastpReport <- fastpReport

        } yield {
          val pastFastpReports = pastSampleResult.toSeq.flatMap(_.fastpReports)
          val pastDemultiplexed =
            pastSampleResult.toSeq.flatMap(_.demultiplexed)
          val pastRunFolders = pastSampleResult.toSeq.flatMap(_.runFolders)

          progressServer.send(
            SampleProcessingFinished(demultiplexedSample.project,
                                     demultiplexedSample.sampleId,
                                     demultiplexedSample.runId))

          Some(
            SampleResult(
              wes = perSampleResultsWES,
              rna = perSampleResultsRNA,
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

  private def wes(sampleForWESAnalysis: PerSamplePerRunFastQ,
                  conf: WESConfiguration,
                  previousUncalibratedBam: Option[Bam])(
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
            sampleForWESAnalysis.withoutRunId,
            reference,
            knownSites.toSet.toStable,
            selectionTargetIntervals,
            dbSnpVcf,
            variantEvaluationIntervals,
            previousUncalibratedBam,
            !conf.doVariantCalls.exists(_ == false),
            minimumWGSCoverage = conf.minimumWGSCoverage,
            minimumTargetCoverage = conf.minimumTargetCoverage,
            contigsFile = contigsFile
          ))(
          ResourceConfig.minimal,
          labels =
            ResourceConfig.projectAndSampleLabel(sampleForWESAnalysis.project,
                                                 sampleForWESAnalysis.sampleId,
                                                 conf.analysisId,
                                                 sampleForWESAnalysis.runId)
        )
      }
      bamPath <- perSampleResultsWES.bam.bam.uri.map(_.toString)
      _ = progressServer.send(
        BamAvailable(sampleForWESAnalysis.project,
                     sampleForWESAnalysis.sampleId,
                     sampleForWESAnalysis.runId.toString,
                     conf.analysisId,
                     bamPath))
      _ <- perSampleResultsWES.gvcf match {
        case Some(vcf) =>
          vcf.vcf.uri.map(_.toString).map { vcfPath =>
            progressServer.send(
              VCFAvailable(sampleForWESAnalysis.project,
                           sampleForWESAnalysis.sampleId,
                           sampleForWESAnalysis.runId.toString,
                           conf.analysisId,
                           vcfPath))
          }
        case _ => Future.successful(())
      }

      wgsMeanCoverage <- AlignmentQC.getWGSMeanCoverage(
        perSampleResultsWES.wgsQC,
        sampleForWESAnalysis.project,
        sampleForWESAnalysis.sampleId)
      _ = progressServer.send(
        CoverageAvailable(sampleForWESAnalysis.project,
                          sampleForWESAnalysis.sampleId,
                          sampleForWESAnalysis.runId,
                          conf.analysisId,
                          wgsMeanCoverage))

    } yield
      (perSampleResultsWES,
       SingleSampleConfiguration(conf.analysisId,
                                 dbSnpVcf,
                                 vqsrTrainingFiles,
                                 conf,
                                 contigsFile))

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
          quantificationGtf = quantificationGtf
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

  private def inProjectQCFolder[T](project: Project)(
      f: TaskSystemComponents => T)(implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("projectQC", project))(f)

  private def inProjectJointCallFolder[T](project: Project,
                                          analysisId: AnalysisId)(
      f: TaskSystemComponents => T)(implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("projects", project, "joint-calls", analysisId))(f)

  private def inDeliverablesFolder[T](f: TaskSystemComponents => T)(
      implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("deliverables"))(f)

}
