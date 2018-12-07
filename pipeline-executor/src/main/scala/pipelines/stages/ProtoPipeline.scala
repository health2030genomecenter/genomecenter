package org.gc.pipelines.stages

import scala.concurrent.{ExecutionContext, Future}
import tasks._
import org.gc.pipelines.application.{
  Pipeline,
  RunfolderReadyForProcessing,
  WESConfiguration,
  RNASeqConfiguration
}
import org.gc.pipelines.model._
import org.gc.pipelines.util.ResourceConfig
import org.gc.pipelines.util.StableSet.syntax
import com.typesafe.scalalogging.StrictLogging

class ProtoPipeline(implicit EC: ExecutionContext)
    extends Pipeline[PerSamplePerRunFastQ, SampleResult]
    with StrictLogging {

  def canProcess(r: RunfolderReadyForProcessing) = {
    r.runConfiguration.automatic
  }

  def getKeysOfDemultiplexedSample(
      d: PerSamplePerRunFastQ): (Project, SampleId, RunId) =
    (d.project, d.sampleId, d.runId)

  def getKeysOfSampleResult(d: SampleResult): (Project, SampleId, RunId) =
    (d.project, d.sampleId, d.lastRunId)

  def processCompletedRun(samples: Seq[SampleResult])(
      implicit tsc: TaskSystemComponents): Future[(RunId, Boolean)] = {
    require(samples.map(_.lastRunId).distinct.size == 1)
    val runId = samples.head.lastRunId

    val fastqsOfThisRun =
      samples
        .flatMap(_.demultiplexed)
        .filter(_.runId == runId)
        .map(_.withoutRunId)

    val sampleQCsWES = samples.flatMap(_.extractWESQCFiles.toSeq)

    inRunQCFolder(runId) { implicit tsc =>
      for {

        _ <- AlignmentQC.runQCTable(
          RunQCTableInput(runId + "." + samples.size,
                          sampleQCsWES.toSet.toStable))(ResourceConfig.minimal)
        _ <- RunQCRNA.runQCTable(
          RunQCTableRNAInput(
            runId + "." + samples.size,
            samples.flatMap(_.rna.toSeq.map(_.star)).toSet.toStable))(
          ResourceConfig.minimal)
        _ <- ReadQC.readQC(
          ReadQCInput(fastqsOfThisRun.toSet.toStable,
                      runId + "." + samples.size))(ResourceConfig.minimal)
      } yield (runId, true)
    }

  }

  def processCompletedProject(samples: Seq[SampleResult])(
      implicit tsc: TaskSystemComponents): Future[(Project, Boolean)] = {
    assert(samples.map(_.project).distinct.size == 1)
    val project = samples.head.project

    val fastqsOfThisRun =
      samples
        .flatMap(_.demultiplexed)
        .filter(_.project == project)
        .map(_.withoutRunId)

    val sampleQCsWES = samples.flatMap(_.extractWESQCFiles.toSeq)

    def projectQC = inProjectQCFolder(project) { implicit tsc =>
      for {

        wes <- AlignmentQC.runQCTable(
          RunQCTableInput(project + "." + samples.size,
                          sampleQCsWES.toSet.toStable))(ResourceConfig.minimal)
        rna <- RunQCRNA.runQCTable(
          RunQCTableRNAInput(
            project + "." + samples.size,
            samples.flatMap(_.rna.toSeq.map(_.star)).toSet.toStable))(
          ResourceConfig.minimal)
        reads <- ReadQC.readQC(
          ReadQCInput(fastqsOfThisRun.toSet.toStable,
                      project + "." + samples.size))(ResourceConfig.minimal)
      } yield (wes, rna, reads)
    }

    for {
      (wes, rna, reads) <- projectQC
      _ <- inDeliverablesFolder { implicit tsc =>
        Delivery.collectDeliverables(
          CollectDeliverablesInput(samples.toSet.toStable,
                                   Set(project -> wes.htmlTable,
                                       project -> rna,
                                       project -> reads.plots).toStable))(
          ResourceConfig.minimal)
      }
    } yield (project, true)
  }

  def demultiplex(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents): Future[Seq[PerSamplePerRunFastQ]] =
    ProtoPipelineStages.executeDemultiplexing(r)

  def processSample(r: RunfolderReadyForProcessing,
                    pastSampleResult: Option[SampleResult],
                    demultiplexedSample: PerSamplePerRunFastQ)(
      implicit tsc: TaskSystemComponents): Future[Option[SampleResult]] = {

    ProtoPipelineStages.parseReadLengthFromRunInfo(r) match {
      case Left(error) =>
        logger.error(s"$error")
        Future.successful(None)
      case Right(readLengths) =>
        logger.info(s"${r.runId} read lengths: ${readLengths.mkString(", ")}")

        val fastpReport = startFastpReports(demultiplexedSample)

        val selectedWESConfiguration = ProtoPipelineStages.selectConfiguration(
          r.runConfiguration.wesProcessing.toSeq.toList,
          demultiplexedSample)

        val selectedRNASeqConfiguration =
          ProtoPipelineStages.selectConfiguration(
            r.runConfiguration.rnaProcessing.toSeq.toList,
            demultiplexedSample)

        val perSampleResultsWES = selectedWESConfiguration.fold(emptyWesResult)(
          wes(
            demultiplexedSample,
            _,
            pastSampleResult.flatMap(_.wes.map(_.uncalibrated)),
          ))

        val perSampleResultsRNA = selectedRNASeqConfiguration.fold(
          emptyRNASeqResult)(rna(demultiplexedSample, _, readLengths))

        for {

          perSampleResultsWES <- perSampleResultsWES
          perSampleResultsRNA <- perSampleResultsRNA

          fastpReport <- fastpReport

        } yield {
          val pastFastpReports = pastSampleResult.toSeq.flatMap(_.fastpReports)
          val pastDemultiplexed =
            pastSampleResult.toSeq.flatMap(_.demultiplexed)
          val pastRunFolders = pastSampleResult.toSeq.flatMap(_.runFolders)

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

  private def wes(samplesForWESAnalysis: PerSamplePerRunFastQ,
                  conf: WESConfiguration,
                  previousUncalibratedBam: Option[Bam])(
      implicit tsc: TaskSystemComponents) =
    for {
      reference <- ProtoPipelineStages.fetchReference(conf.referenceFasta)
      knownSites <- ProtoPipelineStages.fetchKnownSitesFiles(conf)

      selectionTargetIntervals <- ProtoPipelineStages.fetchTargetIntervals(conf)
      dbSnpVcf <- ProtoPipelineStages.fetchDbSnpVcf(conf)
      variantEvaluationIntervals <- ProtoPipelineStages
        .fetchVariantEvaluationIntervals(conf)
      vqsrTrainingFiles <- ProtoPipelineStages.fetchVqsrTrainingFiles(conf)
      perSampleResultsWES <- ProtoPipelineStages.singleSampleWES(
        SingleSamplePipelineInput(
          samplesForWESAnalysis.withoutRunId,
          reference,
          knownSites.toSet.toStable,
          selectionTargetIntervals,
          dbSnpVcf,
          variantEvaluationIntervals,
          previousUncalibratedBam,
          vqsrTrainingFiles
        ))(ResourceConfig.minimal)

    } yield Some(perSampleResultsWES)

  private def rna(
      samplesForRNASeqAnalysis: PerSamplePerRunFastQ,
      conf: RNASeqConfiguration,
      readLengths: Map[ReadType, Int])(implicit tsc: TaskSystemComponents) =
    for {
      gtf <- ProtoPipelineStages.fetchGenemodel(conf)
      reference <- ProtoPipelineStages.fetchReference(conf.referenceFasta)
      perSampleResultsRNA <- ProtoPipelineStages.singleSampleRNA(
        SingleSamplePipelineInputRNASeq(
          samplesForRNASeqAnalysis.withoutRunId,
          reference,
          gtf,
          readLengths.toSeq.toSet.toStable
        ))(ResourceConfig.minimal)

    } yield Some(perSampleResultsRNA)

  private def startFastpReports(perSampleFastQs: PerSamplePerRunFastQ)(
      implicit tsc: TaskSystemComponents): Future[FastpReport] =
    tsc.withFilePrefix(
      Seq("projects",
          perSampleFastQs.project,
          perSampleFastQs.sampleId,
          "fastp",
          perSampleFastQs.runId)) { implicit tsc =>
      Fastp.report(perSampleFastQs)(ResourceConfig.fastp)
    }

  private def inRunQCFolder[T](runId: RunId)(f: TaskSystemComponents => T)(
      implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("runQC", runId))(f)

  private def inProjectQCFolder[T](project: Project)(
      f: TaskSystemComponents => T)(implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("projectQC", project))(f)

  private def inDeliverablesFolder[T](f: TaskSystemComponents => T)(
      implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("deliverables"))(f)

  private val emptyWesResult =
    Future.successful(Option.empty[SingleSamplePipelineResult])

  private val emptyRNASeqResult =
    Future.successful(Option.empty[SingleSamplePipelineResultRNA])

}
