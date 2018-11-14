package org.gc.pipelines.stages

import scala.concurrent.{ExecutionContext, Future}
import tasks._
import org.gc.pipelines.application.{Pipeline, RunfolderReadyForProcessing}
import org.gc.pipelines.model._
import org.gc.pipelines.util.ResourceConfig
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
    assert(samples.map(_.lastRunId).distinct.size == 1)
    val runId = samples.head.lastRunId

    val fastqsOfThisRun =
      samples
        .flatMap(_.demultiplexed)
        .filter(_.runId == runId)
        .map(_.withoutRunId)

    val sampleQCsWES = samples.flatMap(_.extractWESQCFiles.toSeq)

    inRunQCFolder(runId) { implicit tsc =>
      for {

        _ <- AlignmentQC.runQCTable(RunQCTableInput(runId, sampleQCsWES))(
          ResourceConfig.minimal)
        _ <- RunQCRNA.runQCTable(
          RunQCTableRNAInput(runId,
                             samples.flatMap(_.rna.toSeq.map(_.star)).toSet))(
          ResourceConfig.minimal)
        _ <- ReadQC.readQC(ReadQCInput(fastqsOfThisRun.toSet, runId))(
          ResourceConfig.minimal)
      } yield (runId, true)
    }

  }

  def processCompletedProject(samples: Seq[SampleResult])(
      implicit tsc: TaskSystemComponents): Future[(Project, Boolean)] =
    inDeliverablesFolder { implicit tsc =>
      for {
        _ <- Delivery.collectDeliverables(
          CollectDeliverablesInput(samples.toSet))(ResourceConfig.minimal)
      } yield (samples.head.project, true)
    }

  def demultiplex(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents): Future[Seq[PerSamplePerRunFastQ]] =
    ProtoPipelineStages.executeDemultiplexing(r)

  def processSample(r: RunfolderReadyForProcessing,
                    pastSampleResult: Option[SampleResult],
                    demultiplexedSample: PerSamplePerRunFastQ)(
      implicit tsc: TaskSystemComponents): Future[Option[SampleResult]] = {

    val readLengths = ProtoPipelineStages.parseReadLengthFromRunInfo(r)

    logger.info(s"${r.runId} read lengths: ${readLengths.mkString(", ")}")

    for {
      reference <- ProtoPipelineStages.fetchReference(r.runConfiguration)
      knownSites <- ProtoPipelineStages.fetchKnownSitesFiles(r.runConfiguration)
      gtf <- ProtoPipelineStages.fetchGenemodel(r.runConfiguration)
      selectionTargetIntervals <- ProtoPipelineStages.fetchTargetIntervals(
        r.runConfiguration)
      dbSnpVcf <- ProtoPipelineStages.fetchDbSnpVcf(r.runConfiguration)
      variantEvaluationIntervals <- ProtoPipelineStages
        .fetchVariantEvaluationIntervals(r.runConfiguration)

      fastpReport = startFastpReports(demultiplexedSample)

      samplesForWESAnalysis = ProtoPipelineStages.select(
        r.runConfiguration.wesSelector,
        demultiplexedSample)

      samplesForRNASeqAnalysis = ProtoPipelineStages.select(
        r.runConfiguration.rnaSelector,
        demultiplexedSample)

      _ = {
        logger.info(
          s"Demultiplexed samples from run ${r.runId} : $demultiplexedSample")
        logger.info(s"WES samples from run ${r.runId} : $samplesForWESAnalysis")
        logger.info(
          s"RNASEQ samples from run ${r.runId} : $samplesForRNASeqAnalysis")
      }

      perSampleResultsWES = samplesForWESAnalysis.fold(emptyWesResult)(
        wes(
          _,
          reference,
          knownSites.toSeq,
          selectionTargetIntervals,
          dbSnpVcf,
          variantEvaluationIntervals,
          fastpReport.map(Seq(_)),
          pastSampleResult.flatMap(_.wes.map(_.uncalibrated))
        ))

      perSampleResultsRNA = samplesForRNASeqAnalysis.fold(emptyRNASeqResult)(
        rna(_, reference, gtf, readLengths))

      perSampleResultsWES <- perSampleResultsWES
      perSampleResultsRNA <- perSampleResultsRNA

      fastpReport <- fastpReport

    } yield {
      val pastFastpReports = pastSampleResult.toSeq.flatMap(_.fastpReports)
      val pastDemultiplexed = pastSampleResult.toSeq.flatMap(_.demultiplexed)
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

  private def wes(samplesForWESAnalysis: PerSamplePerRunFastQ,
                  reference: ReferenceFasta,
                  knownSites: Seq[VCF],
                  selectionTargetIntervals: BedFile,
                  dbSnpVcf: VCF,
                  variantEvaluationIntervals: BedFile,
                  fastpReports: Future[Seq[FastpReport]],
                  previousUncalibratedBam: Option[Bam])(
      implicit tsc: TaskSystemComponents) =
    for {
      perSampleResultsWES <- ProtoPipelineStages.singleSampleWES(
        SingleSamplePipelineInput(
          samplesForWESAnalysis.withoutRunId,
          reference,
          knownSites.toSet,
          selectionTargetIntervals,
          dbSnpVcf,
          variantEvaluationIntervals,
          previousUncalibratedBam
        ))(ResourceConfig.minimal)

      fastpReports <- fastpReports

    } yield Some(perSampleResultsWES)

  private def rna(
      samplesForRNASeqAnalysis: PerSamplePerRunFastQ,
      reference: ReferenceFasta,
      gtf: GTFFile,
      readLengths: Map[ReadType, Int])(implicit tsc: TaskSystemComponents) =
    for {

      perSampleResultsRNA <- ProtoPipelineStages.singleSampleRNA(
        SingleSamplePipelineInputRNASeq(
          samplesForRNASeqAnalysis.withoutRunId,
          reference,
          gtf,
          readLengths.toSeq.toSet
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

  private def inDeliverablesFolder[T](f: TaskSystemComponents => T)(
      implicit tsc: TaskSystemComponents) =
    tsc.withFilePrefix(Seq("deliverables"))(f)

  private val emptyWesResult =
    Future.successful(Option.empty[SingleSamplePipelineResult])

  private val emptyRNASeqResult =
    Future.successful(Option.empty[SingleSamplePipelineResultRNA])

}
