package org.gc.pipelines.application

import akka.stream.scaladsl.{Source, Sink, Flow, GraphDSL, Broadcast, Merge}
import akka.NotUsed
import akka.stream.{OverflowStrategy, FlowShape, FanOutShape3, Graph}
import tasks._
import com.typesafe.scalalogging.StrictLogging
import akka.actor.ActorSystem
import scala.concurrent.{ExecutionContext, Future}

import org.gc.pipelines.util.ActorSource
import org.gc.pipelines.util.AkkaStreamComponents.{unzipThenMerge, deduplicate}
import org.gc.pipelines.model.{Project, SampleId, RunId}

case class RunFinished(runId: RunId, success: Boolean)
case class ProjectFinished[T](project: Project,
                              success: Boolean,
                              deliverables: Option[T])
case class SampleFinished[T](project: Project,
                             sample: SampleId,
                             runId: RunId,
                             success: Boolean,
                             result: Option[T])

object PipelinesApplication extends StrictLogging {
  def persistCommands(pipelineState: PipelineState)(
      implicit ec: ExecutionContext)
    : PartialFunction[Command, Future[List[RunWithAnalyses]]] = {
    case ReprocessAllRuns =>
      logger.info("Command: reprocess all runs")
      pipelineState.pastRuns
    case Delete(runId) =>
      logger.info(s"Command: delete $runId")
      pipelineState.invalidated(runId).map(_ => Nil)
    case Append(run) =>
      logger.info(s"Command: append ${run.runId}")
      val validationErrors = run.validationErrors
      val valid = validationErrors.isEmpty

      if (!valid) {
        logger.info(
          s"$run is not valid (readable?). Validation errors: $validationErrors")
        Future.successful(Nil)
      } else
        for {
          r <- {
            logger.debug(s"${run.runId} is valid")
            pipelineState.registered(run).map(_.toList)
          }
        } yield r
    case Assign(project, analysisConfiguration) =>
      logger.info(s"Command: assign analaysis to $project")
      val validationErrors = analysisConfiguration.validationErrors
      val valid = validationErrors.isEmpty

      if (!valid) {
        logger.info(
          s"Configuration is not valid. Validation errors: $validationErrors")
        Future.successful(Nil)
      } else {
        logger.info(
          s"Configuration is valid. Assign $project to ${analysisConfiguration.analysisId}")
        pipelineState
          .assigned(project, analysisConfiguration)
          .map(_ => Nil)
      }

    case Unassign(project, analysisId) =>
      logger.info(s"Command: Unassign $project from $analysisId")
      pipelineState
        .unassigned(project, analysisId)
        .map(_ => Nil)
  }

  def foldSample[SampleResult, DemultiplexedSample](
      pipeline: Pipeline[DemultiplexedSample, SampleResult, _],
      processingFinishedListener: akka.actor.ActorRef,
      currentRunConfiguration: RunWithAnalyses,
      pastResultsOfThisSample: Option[SampleResult],
      currentDemultiplexedSample: DemultiplexedSample)(
      implicit tsc: TaskSystemComponents,
      ec: ExecutionContext) =
    pipeline
      .processSample(currentRunConfiguration.run,
                     currentRunConfiguration.analyses,
                     pastResultsOfThisSample,
                     currentDemultiplexedSample)
      .map { result =>
        val (project, sampleId, runId) =
          pipeline.getKeysOfDemultiplexedSample(currentDemultiplexedSample)

        processingFinishedListener ! SampleFinished(project,
                                                    sampleId,
                                                    runId,
                                                    true,
                                                    result)
        result
      }
      .recover {
        case error =>
          logger.error(s"$pipeline failed on $currentDemultiplexedSample",
                       error)

          val (project, sampleId, runId) =
            pipeline.getKeysOfDemultiplexedSample(currentDemultiplexedSample)

          processingFinishedListener ! SampleFinished(project,
                                                      sampleId,
                                                      runId,
                                                      false,
                                                      None)

          pastResultsOfThisSample
      }

  def processSamplesOfCompletedProject[SampleResult](
      pipeline: Pipeline[_, SampleResult, _],
      samples: Seq[SampleResult])(implicit tsc: TaskSystemComponents,
                                  ec: ExecutionContext) = {
    val (project, _, _) = pipeline.getKeysOfSampleResult(samples.head)

    val lastRunOfEachSample = samples.zipWithIndex
      .groupBy {
        case (sample, _) =>
          val (_, sampleId, _) = pipeline.getKeysOfSampleResult(sample)
          sampleId
      }
      .toSeq
      .map {
        case (_, group) =>
          group.last
      }
      .sortBy { case (_, idx) => idx }
      .map { case (sample, _) => sample }

    logger.info(
      s"Per sample processing of $project with ${lastRunOfEachSample.size} samples finished.")
    pipeline.processCompletedProject(lastRunOfEachSample).recover {
      case error =>
        logger.error(
          s"$pipeline failed on $project while processing completed project",
          error)
        (project, false, None)
    }
  }
}

class PipelinesApplication[DemultiplexedSample, SampleResult, Deliverables](
    val commandSource: CommandSource,
    val pipelineState: PipelineState,
    val actorSystem: ActorSystem,
    val taskSystem: TaskSystem,
    val pipeline: Pipeline[DemultiplexedSample, SampleResult, Deliverables],
    blacklist: Set[(Project, SampleId)]
)(implicit EC: ExecutionContext)
    extends StrictLogging {

  private val (processingFinishedListener,
               _processingFinishedSource,
               closeProcessingFinishedSource) =
    ActorSource.make[AnyRef](actorSystem)

  /**
    * This source may be used to monitor the application
    * An actor is already materialized for this, therefore
    * closeProcessingFinishedSource must be called when finished.
    */
  val processingFinishedSource = _processingFinishedSource

  import pipeline.getKeysOfDemultiplexedSample
  import pipeline.getKeysOfSampleResult

  private def getSampleId(s: DemultiplexedSample) = {
    val (project, sampleId, _) = getKeysOfDemultiplexedSample(s)
    (project, sampleId)
  }

  private val previousRuns =
    Source
      .fromFuture(pipelineState.pastRuns)
      .map { runs =>
        runs.foreach { run =>
          logger.info(s"Recovered past runs ${run.runId}")
        }
        runs
      }
      .mapConcat(identity)

  private val persistCommandsFunction =
    PipelinesApplication.persistCommands(pipelineState)

  private def validateCommandAndPersistEvents
    : Flow[Command, RunWithAnalyses, _] =
    Flow[Command]
      .mapAsync(1)(persistCommandsFunction)
      .mapConcat(identity)

  def futureRuns =
    commandSource.commands
      .via(validateCommandAndPersistEvents)

  implicit val taskSystemComponents = taskSystem.components
  implicit val materializer = taskSystemComponents.actorMaterializer

  (previousRuns ++ futureRuns)
    .filter(runWithAnalyses => pipeline.canProcess(runWithAnalyses.run))
    .via(accountAndProcess)
    .via(processCompletedRunsAndProjects)
    .via(watchTermination)
    .to(Sink.ignore)
    .run

  case class Completeds(runs: CompletedRun, projects: CompletedProject)

  /*
   *
   *                             IN [RunfolderReadForProcessing]
   *                             |
   *+----------------------------v-------------------------------------------------------+
   *|                            |                                                       |
   *|                            |                                                       |
   *|                            .------>--+                                             |
   *|                                      |                                             |
   *|                                      |                                             |
   *|                                      |                                             |
   *|                                      v                                             |
   *|                                      |                                             |
   *|                            +-------- | ---[Demux]-Raw-<--+                         |
   *|                            v         |      +------------^----------------+        |
   *|                            |  .______.      |                             |        |
   *|      +------------+        |  |             |     accountWorkDone         |        |
   *|      |            |    +---v--v+ Demuxed    |                             |        |
   *|      |            |    | merge >------------> Accumulates state of runs   |        |
   *|      |  Sample    |    +---^---+ Result     |   and samples               |        |
   *|      |  Processor |        |                +--v---------v----------------+        |
   *|      |            |        |                   |         |                         |
   *|      |            >-Result-+                   |         |                         |
   *|      +------^-----+                            |         |                         |
   *|             |                                  |         |completeds               |
   *|             |                                  |         |                         |
   *|             +----------Demultiplexed sample----.         |                         |
   *|                        accounting of demultiplexed       |                         |
   *|                        sample is done                    |                         |
   *+------------------------------------------------------------------------------------+
   *                                                           |
   *                                                           OUT [Completeds]
   */
  def accountAndProcess: Flow[RunWithAnalyses, Completeds, _] =
    Flow.fromGraph(GraphDSL
      .create(accountWorkDone, demultiplex, sampleProcessing)((_, _, _)) {
        implicit builder => (accountWorkDone, demultiplex, sampleProcessing) =>
          import GraphDSL.Implicits._

          type DM = (RunWithAnalyses, Seq[DemultiplexedSample])

          val merge =
            builder.add(Merge[Stage](3))

          val mapToRaw =
            builder.add(Flow[RunWithAnalyses].map(runFolder => Raw(runFolder)))
          mapToRaw.out ~> merge.in(2)

          accountWorkDone.out0 ~> Flow[DM]
            .mapConcat {
              case (run, samples) =>
                samples.map(s => (run, s)).toList
            } ~> sampleProcessing ~> Flow[SampleResult]
            .map(ProcessedSample(_)) ~> merge.in(1)

          merge.out ~> accountWorkDone.in

          accountWorkDone.out2 ~> demultiplex ~> Flow[DM].map {
            case (run, demultiplexedSamples) =>
              Demultiplexed(run, demultiplexedSamples)
          } ~> merge.in(0)

          FlowShape(mapToRaw.in, accountWorkDone.out1)
      })

  def isOnBlacklist(sample: DemultiplexedSample): Boolean = {
    val (project, sampleId, _) = getKeysOfDemultiplexedSample(sample)
    blacklist.contains((project, sampleId))
  }

  def demultiplex
    : Flow[RunWithAnalyses, (RunWithAnalyses, Seq[DemultiplexedSample]), _] =
    Flow[RunWithAnalyses]
      .mapAsync(1000) { runWithAnalyses =>
        val run = runWithAnalyses.run
        logger.debug(s"Call pipelines demultiplex method for run ${run.runId}")
        for {
          samples <- pipeline.demultiplex(run).recover {
            case error =>
              logger.error(s"$pipeline failed on ${run.runId}", error)
              Nil
          }
        } yield {
          val samplesPassingBlacklist = samples.filterNot(isOnBlacklist)
          logger.info(
            s"Demultiplexing of ${run.runId} with ${samplesPassingBlacklist.size} (${samples.size} before blacklist) done.")
          runWithAnalyses -> samplesPassingBlacklist
        }
      }

  def accountWorkDone
    : Graph[FanOutShape3[Stage,
                         (RunWithAnalyses, Seq[DemultiplexedSample]),
                         Completeds,
                         RunWithAnalyses],
            NotUsed] = {

    val state = Flow[Stage]
      .scan(StateOfUnfinishedSamples.empty) {
        case (state, Demultiplexed(runWithAnalyses, demultiplexedSamples))
            if demultiplexedSamples.nonEmpty =>
          val sampleIds = demultiplexedSamples.map(ds => getSampleId(ds))
          logger.info(
            s"Got demultiplexed ${sampleIds.size} samples from ${runWithAnalyses.run.runId} (${sampleIds
              .mkString(", ")})")
          state.addNewDemultiplexedSamples(
            (runWithAnalyses, demultiplexedSamples))

        case (state, Demultiplexed(run, _)) =>
          logger.info(s"Demultiplexed 0 samples from ${run.runId}.")
          state.removeFromUnfinishedDemultiplexing(run.runId)

        case (state, ProcessedSample(processedSample)) =>
          logger.info(
            s"Processed sample ${pipeline.getKeysOfSampleResult(processedSample)}")
          state.finish(processedSample)

        case (state, Raw(runWithAnalyses)) =>
          logger.info(s"Got new run ${runWithAnalyses.run.runId}")
          state.addNewRun(runWithAnalyses)
      }

    GraphDSL
      .create(state) { implicit builder => stateScan =>
        import GraphDSL.Implicits._

        val broadcast =
          builder.add(Broadcast[StateOfUnfinishedSamples](3))

        val completeds = builder.add(Flow[StateOfUnfinishedSamples].map(state =>
          Completeds(state.completedRun, state.completedProject)))

        val sendToSampleProcessing = builder.add(
          Flow[StateOfUnfinishedSamples]
            .map(_.sendToSampleProcessing)
            .filter(_.nonEmpty)
            .map(_.get)
        )

        val sendToDemultiplexing = builder.add(
          Flow[StateOfUnfinishedSamples]
            .map(_.sendToDemultiplexing)
            .via(deduplicate)
            .mapConcat(_.toList)
            .map { runFolder =>
              logger.info(s"Sending ${runFolder.runId} to demultiplexing.")
              runFolder
            }
        )

        stateScan.out ~> broadcast.in

        broadcast.out(0) ~> completeds.in
        broadcast.out(1) ~> sendToSampleProcessing.in
        broadcast.out(2) ~> sendToDemultiplexing.in

        new FanOutShape3(stateScan.in,
                         sendToSampleProcessing.out,
                         completeds.out,
                         sendToDemultiplexing.out)
      }

  }

  def processCompletedRunsAndProjects =
    Flow[Completeds]
      .buffer(size = 10000, OverflowStrategy.backpressure)
      .map { case Completeds(runs, projects) => (runs, projects) }
      .via(unzipThenMerge(processCompletedRuns, processCompletedProjects))

  def processCompletedRuns =
    Flow[CompletedRun]
      .filter(_.samples.nonEmpty)
      .map {
        case CompletedRun(samples) =>
          val (_, _, runId) = getKeysOfSampleResult(samples.head)
          logger.info(s"Run $runId finished with ${samples.size} samples.")
          (runId, samples)
      }
      .groupBy(maxSubstreams = 10000, { case (runId, _) => runId })
      .via(deduplicate)
      .buffer(1, OverflowStrategy.dropHead)
      .mapAsync(1) {
        case (runId, samples) =>
          pipeline.processCompletedRun(samples).recover {
            case error =>
              logger.error(
                s"$pipeline failed on ${runId} while processing completed run",
                error)
              (runId, false)
          }
      }
      .map {
        case (runId, success) =>
          processingFinishedListener ! RunFinished(runId, success)
          logger.debug(
            s"Processing of completed run $runId finished (with or without error). Success: $success")
          runId
      }
      .mergeSubstreams

  def processCompletedProjects =
    Flow[CompletedProject]
      .filter(_.samples.nonEmpty)
      .map {
        case CompletedProject(samples) =>
          val (project, _, _) = getKeysOfSampleResult(samples.head)
          (project, samples)
      }
      .groupBy(maxSubstreams = 10000, { case (project, _) => project })
      .via(deduplicate)
      // A buffer of size one with DropHead overflow strategy ensures
      // that the stream is always pulled and the latest element is processed.
      // If there is a newer element, then we want to drop the previously buffered elements
      // because we care for the latest
      .buffer(1, OverflowStrategy.dropHead)
      .mapAsync(1000) {
        case (_, samples) =>
          PipelinesApplication.processSamplesOfCompletedProject(pipeline,
                                                                samples)
      }
      .map {
        case (project, success, deliverables) =>
          processingFinishedListener ! ProjectFinished(project,
                                                       success,
                                                       deliverables)
          logger.debug(
            s"Processing of completed project $project finished (with or without error). Success: $success.")
          project
      }
      .mergeSubstreams

  def sampleProcessing
    : Flow[(RunWithAnalyses, DemultiplexedSample), SampleResult, _] = {

    val maxConcurrentlyProcessedSamples = 10000

    Flow[(RunWithAnalyses, DemultiplexedSample)]
      .map {
        case value @ (runFolder, demultiplexedSample) =>
          val keys = getKeysOfDemultiplexedSample(demultiplexedSample)
          logger.debug(
            s"SampleProcessor received sample (before groupby) ${runFolder.runId} $keys")
          value
      }
      .groupBy(maxSubstreams = maxConcurrentlyProcessedSamples, {
        case (_, demultiplexedSample) => getSampleId(demultiplexedSample)
      })
      .map {
        case value @ (runFolder, demultiplexedSample) =>
          val keys = getKeysOfDemultiplexedSample(demultiplexedSample)
          logger.debug(
            s"SampleProcessor received sample (after groupby) ${runFolder.runId} $keys")
          value
      }
      .scanAsync(
        List.empty[(SampleResult, RunWithAnalyses, DemultiplexedSample)]) {
        case (pastResultsOfThisSample,
              (currentRunConfiguration, currentDemultiplexedSample)) =>
          val (runsBeforeThis, runsAfterInclusive) =
            pastResultsOfThisSample.span {
              case (_, pastRunFolder, _) =>
                pastRunFolder.run.runId != currentRunConfiguration.run.runId
            }

          val runsAfterThis = runsAfterInclusive.drop(1).map {
            case (_, runFolder, demultiplexedSamples) =>
              (runFolder, demultiplexedSamples)
          }

          val runsToReApply = ((currentRunConfiguration,
                                currentDemultiplexedSample)) +: runsAfterThis

          logger.info(
            s"Processing sample: ${getSampleId(currentDemultiplexedSample)}. RunsBefore: ${runsBeforeThis
              .map(_._2.run.runId)}. RunsToReapply: ${runsToReApply.map(_._1.run.runId)}.")

          runsToReApply.foldLeft(Future.successful(runsBeforeThis)) {
            case (pastIntermediateResults,
                  (currentRunConfiguration, currentDemultiplexedSample)) =>
              for {
                pastIntermediateResults <- pastIntermediateResults

                lastSampleResult = pastIntermediateResults.lastOption.map {
                  case (sampleResult, _, _) => sampleResult
                }

                newSampleResult <- PipelinesApplication.foldSample(
                  pipeline,
                  processingFinishedListener,
                  currentRunConfiguration,
                  lastSampleResult,
                  currentDemultiplexedSample)
              } yield
                newSampleResult match {
                  case None => pastIntermediateResults
                  case Some(newSampleResult) =>
                    pastIntermediateResults :+ ((newSampleResult,
                                                 currentRunConfiguration,
                                                 currentDemultiplexedSample))
                }

          }

      }
      .async
      .mergeSubstreams
      .filter(_.nonEmpty)
      .map(_.last._1)

  }

  def watchTermination[T] =
    Flow[T].watchTermination() {
      case (mat, future) =>
        future.onComplete {
          case result =>
            result.failed.foreach { e =>
              logger.error("Unexpected exception ", e)
            }
            closeProcessingFinishedSource()
            taskSystem.shutdown
            actorSystem.terminate
        }
        mat
    }

  case class CompletedProject(samples: Seq[SampleResult]) {
    require(samples
              .map(sample => getKeysOfSampleResult(sample)._1: Project)
              .distinct
              .size <= 1,
            s"More than one project. Programmer error. $samples")
  }
  case class CompletedRun(samples: Seq[SampleResult])

  sealed trait Stage
  case class Raw(run: RunWithAnalyses) extends Stage
  case class Demultiplexed(run: RunWithAnalyses,
                           demultiplexed: Seq[DemultiplexedSample])
      extends Stage
  case class ProcessedSample(sampleResult: SampleResult) extends Stage

  object StateOfUnfinishedSamples {
    def empty = StateOfUnfinishedSamples(Nil, Set(), Set(), Seq(), None, Nil)
  }

  case class StateOfUnfinishedSamples(
      runFoldersOnHold: Seq[RunWithAnalyses],
      unfinishedDemultiplexingOfRunIds: Set[RunId],
      unfinished: Set[(Project, SampleId, RunId)],
      finished: Seq[SampleResult],
      sendToSampleProcessing: Option[
        (RunWithAnalyses, Seq[DemultiplexedSample])],
      sendToDemultiplexing: Seq[RunWithAnalyses])
      extends StrictLogging {

    private def removeSamplesOfCompletedRuns = {
      val finishedSamples = completedRun.samples.toSet
      val cleared =
        finished.filterNot(sample => finishedSamples.contains(sample))
      copy(finished = cleared)
    }

    private def releaseRunsOnHold = {
      val releasableRunIds = runFoldersOnHold
        .filter { run =>
          val runId = run.runId
          val completedRunIds =
            this.completedRun.samples
              .map(sampleResult => getKeysOfSampleResult(sampleResult)._3)
              .toSet
          completedRunIds.contains(runId)
        }
        .map(_.runId)
        .toSet

      val uniqueRunsOnHold = runFoldersOnHold.zipWithIndex
        .groupBy { case (runFolder, _) => runFolder.runId }
        .toSeq
        .map { case (_, group) => group.sortBy(_._2).map(_._1).last }

      val (sendToDemultiplexing, newRunFoldersOnHold) =
        uniqueRunsOnHold.partition(runFolder =>
          releasableRunIds.contains(runFolder.runId))

      val runIdsToDemultiplex = uniqueRunsOnHold.map(_.runId).toSet

      copy(
        sendToDemultiplexing = sendToDemultiplexing,
        runFoldersOnHold = newRunFoldersOnHold,
        unfinishedDemultiplexingOfRunIds =
          unfinishedDemultiplexingOfRunIds ++ uniqueRunsOnHold
            .map(_.runId),
        finished = finished.filterNot(sampleResult =>
          runIdsToDemultiplex.contains(getKeysOfSampleResult(sampleResult)._3))
      )

    }

    def removeFromUnfinishedDemultiplexing(runId: RunId) = {
      val runRemovedFromUnfinishedDemultiplexing = copy(
        runFoldersOnHold = runFoldersOnHold
          .filterNot(_.runId == runId),
        unfinishedDemultiplexingOfRunIds = unfinishedDemultiplexingOfRunIds - runId,
        sendToSampleProcessing = None,
        sendToDemultiplexing = Nil
      )
      val onHold = runFoldersOnHold.filter(_.runId == runId)
      logger.info(
        s"Removed ${onHold.size} runs from hold and re-add the last with id $runId ")

      onHold.lastOption match {
        case None => runRemovedFromUnfinishedDemultiplexing
        case Some(run) =>
          runRemovedFromUnfinishedDemultiplexing
            .addNewRun(run)
      }

    }

    def addNewRun(run: RunWithAnalyses) =
      if (unfinishedDemultiplexingOfRunIds.contains(run.runId)) {
        logger.info(s"Put run on hold: ${run.runId}.")
        copy(runFoldersOnHold = runFoldersOnHold :+ run,
             sendToSampleProcessing = None,
             sendToDemultiplexing = Nil)
      } else {
        logger.debug(s"Set state to send to demultiplexing: ${run.runId}.")
        removeSamplesOfCompletedRuns.copy(
          unfinishedDemultiplexingOfRunIds = unfinishedDemultiplexingOfRunIds + run.runId,
          sendToSampleProcessing = None,
          sendToDemultiplexing = List(run))
      }

    def addNewDemultiplexedSamples(
        samples: (RunWithAnalyses, Seq[DemultiplexedSample]))
      : StateOfUnfinishedSamples = {
      val keys = samples._2.map(getKeysOfDemultiplexedSample)
      logger.debug(s"Add new demultiplexed samples: $keys.")
      copy(unfinished = unfinished ++ keys,
           sendToSampleProcessing = Some(samples),
           sendToDemultiplexing = Nil)
    }

    def finish(processedSample: SampleResult): StateOfUnfinishedSamples = {
      val keysOfFinishedSample = getKeysOfSampleResult(processedSample)
      logger.debug(
        s"Accunting the completion of sample processing of $keysOfFinishedSample.")
      copy(
        unfinished = unfinished.filterNot(_ == keysOfFinishedSample),
        finished = finished :+ processedSample,
        unfinishedDemultiplexingOfRunIds = unfinishedDemultiplexingOfRunIds
          .filterNot(_ == keysOfFinishedSample._3),
        sendToSampleProcessing = None,
        sendToDemultiplexing = Nil
      ).releaseRunsOnHold
    }

    private def completed[T](extractKey: ((Project, SampleId, RunId)) => T) =
      finished.lastOption match {
        case None => Nil
        case Some(lastFinished) =>
          val keysOfUnfinishedSamples: Set[T] = unfinished.map(extractKey)
          val keyOfLastFinishedSample: T = extractKey(
            getKeysOfSampleResult(lastFinished))
          val existRemaining =
            keysOfUnfinishedSamples.contains(keyOfLastFinishedSample)
          logger.debug(
            s"Finished processing ${getKeysOfSampleResult(lastFinished)} . Remaining runs or projects with unfinished samples: $keysOfUnfinishedSamples")
          if (!existRemaining)
            finished.filter { sampleResult =>
              extractKey(getKeysOfSampleResult(sampleResult)) == keyOfLastFinishedSample
            } else Nil
      }

    def completedRun: CompletedRun =
      CompletedRun(completed(_._3))

    def completedProject: CompletedProject =
      CompletedProject(completed(_._1))
  }

}
