package org.gc.pipelines.application

import akka.stream.scaladsl.{Source, Sink, Flow, GraphDSL, Broadcast, Merge}
import akka.NotUsed
import akka.stream.{OverflowStrategy, FlowShape, FanOutShape3, Graph}
import tasks._
import com.typesafe.scalalogging.StrictLogging
import akka.actor.ActorSystem
import scala.concurrent.{ExecutionContext, Future}

import org.gc.pipelines.util.ActorSource
import org.gc.pipelines.util.traverseAll
import org.gc.pipelines.util.AkkaStreamComponents.{deduplicate}
import org.gc.pipelines.model.{Project, SampleId, RunId}
import akka.stream.ActorMaterializer

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

  /* Validates commands and persists events into `pipelineState`*/
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

  /* Processes a sample with respect to a configuration and potential past
   * results of the same sample
   */
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
      s"Per sample processing of $project with ${lastRunOfEachSample.size} sample-run pairs finished.")
    pipeline.processCompletedProject(lastRunOfEachSample).recover {
      case error =>
        logger.error(
          s"$pipeline failed on $project while processing completed project",
          error)
        (project, false, None)
    }
  }
}

/* Integrates the main components of the application
 * Starts sample processing in reaction to commands
 *
 * All domain specific bioinformatic steps are abstracted out to
 * Pipeline
 *
 * This class starts a closed stream processing graph which runs the
 * command source and executes the processing steps in turn of those commands.
 *
 * In addition to persisting events to pipelineState this class maintains
 * an in memory ephemeral state (in the scan Flow of `accountWorkDone`) which
 * keeps track of which sample, run or project is being processed. If a
 * command is received to process the run twice, this run is deferred from processing
 * until the first invocation is finished.
 *
 */
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
          logger.info(s"Recovered past run ${run.runId}")
        }
        runs
      }

  private val persistCommandsFunction =
    PipelinesApplication.persistCommands(pipelineState)

  private def validateCommandAndPersistEvents
    : Flow[Command, List[RunWithAnalyses], _] =
    Flow[Command]
      .mapAsync(1)(persistCommandsFunction)

  def futureRuns =
    commandSource.commands
      .via(validateCommandAndPersistEvents)

  implicit val taskSystemComponents = taskSystem.components
  implicit val materializer = {
    implicit val as = actorSystem
    ActorMaterializer()
  }

  /* This is a side effecting expression (the .run at the end) which constructs and
   * runs the stream processing engine (see akka stream)
   *
   * The rest of the fields in this class are definitions for stages in this graph.
   *
   * This linear flow consists of 4 parts:
   *
   * 1. The source of batches of runs.
   *    Runs added in a previous invocation of the pipeline are recovered from
   *    the persisted PipelineState and form a single batch
   *    Upcoming future runs each form a single batch
   *
   * 2. The sample processing and accounting stage.
   *    This is a complicated stage which ensures that all runs ever seen are
   *    demultiplexed and all samples of a project ever seen are processed.
   *    Once all samples of a run is processed or all samples or a project is
   *    processed this stage emits a signal downstream.
   *
   * 3. The run and project completion stage
   *    This stage creates run-wide and project-wide summary statistics, tables,
   *    delivery lists, and if needed does additional processing (e.g. joint calls)
   *
   * 4. The sink
   *    At this point the elements in the stream are ignored. Results are communicated
   *    in side channels to the outside world (i.e. the filesystem and the
   *    http interface)
   */
  (previousRuns ++ futureRuns)
    .filter(_.nonEmpty)
    .via(accountAndProcess)
    .via(processCompletedRunsAndProjects)
    .via(watchTermination)
    .to(Sink.ignore)
    .run

  case class Completeds(runs: Option[CompletedRun],
                        projects: Option[CompletedProject])

  /* Accounting and processing stage
   *
   * This stage receives batches of runs, demultiplexes them, processes the samples,
   * and once all samples of a project and/or a run is processed signals the
   * project/run completion downstream.
   *
   * Major parts of this stage;
   * 1. sample processor flow
   *    This flow receives demultiplexed samples and processes them according to the
   *    configurations registered in the pipeline.
   *    The sample processing flow ensures that at any given moment maximum one
   *    instance of the sample processing tasks are running for a given sample.
   *    This flow demultiplexes processing using a bounded groupBy stream operator.
   *
   *
   * 2. demultiplexing flow
   *    This flow receives batches of runs and demultiplexes them in batches.
   *
   *
   * 3. Accounting flow
   *    Both the sample processor flow and the demultiplexing flow receive their
   *    input from the accounting stage and also send back their output to the
   *    accounting flow (via a merge operation). This is needed such that the
   *    accounting flow knows which samples or runs are still have to be processed
   *    and when can it emit the project/run completion signals.
   *    The accouting flow also ensures that at any given time maximum one instance
   *    of the same run is being processed: if the same run is received multiple
   *    times it holds back the processing.
   *    This flow bears a state and is implemented as a scan and a broadcast.
   *
   *
   *    The buffers are are very important because they break deadlocks arising on cycles.
   *    Both sampleprocessing and demultiplexing are part of a cycle in the graph.
   *    Without a buffer this can form a deadlock. To see this consider that akka-stream
   *    is pull based and elements are pulled out only if there is a need.
   *    E.g a cycle of length one with a map stage will immediately deadlock because the
   *    map will signal the need only if being pulled, and it never pulls itself.
   *    Placing a buffer is like a temporary sink which pulls.
   *    (In akka's documentation this pull-based mechanism is referred to as backpressure.)
   *
   *
   *                                            IN [RunfolderReadyForProcessing]
   * +-------------------------------------------+-------------------------------------------+
   * |                                           |                                           |
   * |                                           |                                           |
   * |                                           v                                           |
   * |   +------------------+             +------+------+                +----------------+  |
   * |   |                  >--->BUFF>---->   Merge     <------<BUFF<----<                |  |
   * |   | Sample processor |             +------v------+                |   Demultiplex  |  |
   * |   |                  |                    |                       |                |  |
   * |   |                  |           [Raw/Processed/Demuxed]          |                |  |
   * |   |                  |                    |                       |                |  |
   * |   |                  |       +------------v---------------+       |                |  |
   * |   |                  |       |                            |       |                |  |
   * |   |                  |       |  accountWorkDone           |       |                |  |
   * |   |                  +<------<                            >------>|                |  |
   * |   |            [Demultiplexed sample]                  [Raw runfolder]             |  |
   * |   +------------------+       +-------------+--------------+       +----------------+  |
   * |                                            |                                          |
   * |                                            v                                          |
   * +---------------------------------------------------------------------------------------+
   *                                              |
   *                                              v
   *                                              OUT [Completeds]
   *
   *
   *
   */
  def accountAndProcess: Flow[Seq[RunWithAnalyses], Completeds, _] =
    Flow.fromGraph(
      GraphDSL
        .create(accountWorkDone, demultiplex, sampleProcessing)((_, _, _)) {
          implicit builder =>
            (accountWorkDone, demultiplex, sampleProcessing) =>
              import GraphDSL.Implicits._

              type DM = (RunWithAnalyses, Seq[DemultiplexedSample])

              val merge =
                builder.add(Merge[Stage](3))

              val mapToRaw =
                builder.add(Flow[Seq[RunWithAnalyses]].map(runs => Raw(runs)))
              mapToRaw.out ~> merge.in(2)

              accountWorkDone.out0 ~> Flow[DM]
                .mapConcat {
                  case (run, samples) =>
                    samples.map(s => (run, s)).toList
                } ~> sampleProcessing ~> Flow[(Option[SampleResult],
                                               DemultiplexedSample)]
                .map(ProcessedSample.tupled)
                .buffer(1, OverflowStrategy.backpressure) ~> merge.in(1)

              merge.out ~> accountWorkDone.in

              accountWorkDone.out2 ~> demultiplex ~> Flow[Seq[DM]]
                .map { demultiplexedRuns =>
                  Demultiplexed(demultiplexedRuns)
                }
                .buffer(1, OverflowStrategy.backpressure) ~> merge.in(0)

              FlowShape(mapToRaw.in, accountWorkDone.out1)
        })

  def isOnBlacklist(sample: DemultiplexedSample): Boolean = {
    val (project, sampleId, _) = getKeysOfDemultiplexedSample(sample)
    blacklist.contains((project, sampleId))
  }

  def demultiplex: Flow[Seq[RunWithAnalyses],
                        Seq[(RunWithAnalyses, Seq[DemultiplexedSample])],
                        _] =
    Flow[Seq[RunWithAnalyses]]
      .mapAsync(1000) { runsWithAnalyses =>
        traverseAll(runsWithAnalyses) { runWithAnalyses =>
          val run = runWithAnalyses.run
          logger.debug(
            s"Call pipelines demultiplex method for run ${run.runId}")
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
      }

  /* Accounting stage
   *
   * This stage of formed of a scan which accumulates the state,
   * and a broadcast which copies (forks) emitted elements towards
   * sample processing, demultiplexing or completion.
   * Each flow after the broadcast takes only the elements it needs from the
   * accumulated state.
   *
   *                                      IN
   *                                      |
   *                                      v
   *                           +----------+--------------+
   *                           |                         |
   *                           |   State-scan            |
   *                           |                         |
   *                           +----------+--------------+
   *                                      |
   *                                      v
   * +----------------------+      +------+------+          +--------------------+
   * |sendToSampleProcessing+<-----+  Broadcast  +--------->+sendToDemultiplexing|
   * +----------------------+      +------+------+          +--------------------+
   *                                      |
   *                                      v
   *                           +----------+--------------+
   *                           |   completeds            |
   *                           +-------------------------+
   *
   */
  def accountWorkDone
    : Graph[FanOutShape3[Stage,
                         (RunWithAnalyses, Seq[DemultiplexedSample]),
                         Completeds,
                         Seq[RunWithAnalyses]],
            NotUsed] = {

    val state = Flow[Stage]
      .scan(StateOfUnfinishedSamples.empty) {
        case (state, Demultiplexed(demultiplexedRuns)) =>
          val clearedState = state.copy(
            sendToDemultiplexing = Nil,
            sendToSampleProcessing = Nil,
            sendToCompleteds = None
          )
          demultiplexedRuns.foldLeft(clearedState) {
            case (state, (runWithAnalyses, demultiplexedSamples))
                if demultiplexedSamples.nonEmpty =>
              val sampleIds = demultiplexedSamples.map(ds => getSampleId(ds))
              logger.info(
                s"Got demultiplexed ${sampleIds.size} samples from ${runWithAnalyses.run.runId} (${sampleIds
                  .mkString(", ")})")
              state.addNewDemultiplexedSamples(
                (runWithAnalyses, demultiplexedSamples))

            case (state, (run, _)) =>
              logger.info(s"Demultiplexed 0 samples from ${run.runId}.")
              state.removeFromUnfinishedProcessing(run.runId)

          }

        case (state, ProcessedSample(processedSample, demultiplexedSample)) =>
          logger.info(
            s"Processed sample ${pipeline.getKeysOfDemultiplexedSample(demultiplexedSample)}")
          state.finish(processedSample, demultiplexedSample)

        case (state, Raw(runsWithAnalyses)) =>
          logger.info(
            s"Got new runs ${runsWithAnalyses.map(_.run.runId).mkString(", ")}")
          state.addNewRuns(runsWithAnalyses)
      }

    GraphDSL
      .create(state) { implicit builder => stateScan =>
        import GraphDSL.Implicits._

        val broadcast =
          builder.add(Broadcast[StateOfUnfinishedSamples](3))

        val completeds = builder.add(
          Flow[StateOfUnfinishedSamples]
            .map(state => state.sendToCompleteds)
            .filter(_.isDefined)
            .map(_.get)
            .via(deduplicate))

        val sendToSampleProcessing = builder.add(
          Flow[StateOfUnfinishedSamples]
            .map(_.sendToSampleProcessing)
            .mapConcat(_.toList)
        )

        val sendToDemultiplexing = builder.add(
          Flow[StateOfUnfinishedSamples]
            .map(_.sendToDemultiplexing)
            .via(deduplicate)
            .filter(_.nonEmpty)
            .map { runFolders =>
              logger.info(
                s"Sending ${runFolders.map(_.runId)} to demultiplexing.")
              runFolders
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
      .buffer(size = 10000, OverflowStrategy.fail)
      .map { case Completeds(runs, projects) => (runs, projects) }
      .alsoTo(processCompletedRuns)
      .map { case (_, completedProject) => completedProject }
      .filter(_.isDefined)
      .map(_.get)
      .via(processCompletedProjects)

  import scala.concurrent.duration._

  def processCompletedRuns =
    Flow[(Option[CompletedRun], Option[CompletedProject])]
      .map { case (completedRun, _) => completedRun }
      .filter(_.isDefined)
      .map(_.get)
      .filter(_.samples.nonEmpty)
      .map {
        case CompletedRun(samples) =>
          val (_, _, runId) = getKeysOfSampleResult(samples.head)
          logger.info(s"Run $runId finished with ${samples.size} samples.")
          processingFinishedListener ! RunFinished(runId, true)
          (runId, samples)
      }
      .groupBy(maxSubstreams = 100000, { case (runId, _) => runId })
      .via(deduplicate)
      .mergeSubstreams
      .scan(Map.empty[RunId, Seq[SampleResult]]) {
        case (map, (runId, samples)) =>
          map.updated(runId, samples)
      }
      .buffer(1, OverflowStrategy.dropHead)
      .throttle(1,
                1 minute,
                maximumBurst = 1,
                mode = akka.stream.ThrottleMode.Shaping)
      .mapAsync(1) { completedRuns =>
        logger.debug(s"Completing ${completedRuns.size} runs.")
        pipeline.processCompletedRuns(completedRuns).recover {
          case error =>
            logger.error(
              s"$pipeline failed while processing completed runs ${completedRuns.toSeq.map(_._1).mkString(", ")}",
              error)
            ()
        }
      }
      .to(Sink.ignore)

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
      .mapAsync(1) {
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

  def sampleProcessing: Flow[(RunWithAnalyses, DemultiplexedSample),
                             (Option[SampleResult], DemultiplexedSample),
                             _] = {

    /* Once the number of all time samples surpasses this number
     * the flow will throw an exception and stop.
     * This number may be increased up to Int.MaxValue-10 at the expense of some
     * heap
     */
    val maxTotalAccumulatedSamples = 1000000

    Flow[(RunWithAnalyses, DemultiplexedSample)]
      .scan((Set.empty[(Project, SampleId)],
             Option.empty[(RunWithAnalyses, DemultiplexedSample)])) {
        case ((samplesSoFar, _), elem) =>
          val demultiplexedSample = elem._2
          val sampleId = (getSampleId(demultiplexedSample))
          val withNewSample = samplesSoFar + sampleId
          logger.debug(s"Samples so far: ${withNewSample.size}")
          (withNewSample, Some(elem))
      }
      .filter(_._2.isDefined)
      .map(_._2.get)
      .map {
        case value @ (runFolder, demultiplexedSample) =>
          val keys = getKeysOfDemultiplexedSample(demultiplexedSample)
          logger.debug(
            s"SampleProcessor received sample (before groupby) ${runFolder.runId} $keys")
          value
      }
      .groupBy(maxSubstreams = maxTotalAccumulatedSamples, {
        case (_, demultiplexedSample) => getSampleId(demultiplexedSample)
      })
      .map {
        case value @ (runFolder, demultiplexedSample) =>
          val keys = getKeysOfDemultiplexedSample(demultiplexedSample)
          logger.debug(
            s"SampleProcessor received sample (after groupby) ${runFolder.runId} $keys")
          value
      }
      // The following buffer separately buffers each sample
      // thus the groupBy is pulled unless the next sample would go into a
      // bucket with an already full buffer.
      // This is needed because groupBy is synchronous and otherwise samples were
      // not processed in parallel.
      .buffer(size = 100, OverflowStrategy.backpressure)
      .scanAsync(List
        .empty[(Option[SampleResult], RunWithAnalyses, DemultiplexedSample)]) {
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

                lastSampleResult = pastIntermediateResults.lastOption.flatMap {
                  case (sampleResult, _, _) => sampleResult
                }

                newSampleResult <- PipelinesApplication.foldSample(
                  pipeline,
                  processingFinishedListener,
                  currentRunConfiguration,
                  lastSampleResult,
                  currentDemultiplexedSample)
              } yield
                pastIntermediateResults :+ ((newSampleResult,
                                             currentRunConfiguration,
                                             currentDemultiplexedSample))

          }

      }
      .mergeSubstreams
      .filter(_.nonEmpty)
      .map { scans =>
        val (result, _, demultiplexedSample) = scans.last
        (result, demultiplexedSample)
      }

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
  case class Raw(runs: Seq[RunWithAnalyses]) extends Stage
  case class Demultiplexed(
      demultiplexedRuns: Seq[(RunWithAnalyses, Seq[DemultiplexedSample])])
      extends Stage
  case class ProcessedSample(sampleResult: Option[SampleResult],
                             demultiplexedSample: DemultiplexedSample)
      extends Stage

  object StateOfUnfinishedSamples {
    def empty =
      StateOfUnfinishedSamples(Nil, Set(), Set(), Seq(), Nil, Nil, None)
  }

  /* State held in the accounting flow
   *
   * This is the state that is scanned over the incoming elemens of the accounting
   * flow.
   *
   * Some members are to hold state, while some members serve as emission fields
   * whose content is sent downstream. To understand this consider that this class is
   * used in a scan operator followed by a broadcast.
   *
   * State members:
   * These members are marked as private because they are only accessed or changed via
   * helper methods.
   *
   * - runFoldersOnHold: run folders received at any moment when the same run folder is
   *   still being processed are queued up here. They are sent to demultiplexing once the
   *   first instance if completed
   * - unfinishedDemultiplexingOfRunIds: set of runids which are being demultiplexed at the moment
   * - unfinished: set of sample which are being processed at the moment
   * - finished: accumulates sample results of finished samples of incomplete projects.
   *   Project completion will use this data.
   *
   *
   * Emission members:
   * The whole StateOfUnfinishedSamples is broadcasted (copied) to the demultiplexing
   * and sample processing flows, which project to the corresponding fields.
   * - sendToSampleProcessing
   * - sendToDemultiplexing
   * - sendToCompleteds
   */
  case class StateOfUnfinishedSamples(
      private val runFoldersOnHold: Seq[RunWithAnalyses],
      private val unfinishedProcessingOfRunIds: Set[RunId],
      private val unfinished: Set[(Project, SampleId, RunId)],
      private val finished: Seq[SampleResult],
      sendToSampleProcessing: Seq[(RunWithAnalyses, Seq[DemultiplexedSample])],
      sendToDemultiplexing: Seq[RunWithAnalyses],
      sendToCompleteds: Option[Completeds])
      extends StrictLogging {

    def removeFromUnfinishedProcessing(runId: RunId) = {
      val runRemovedFromUnfinishedDemultiplexing = copy(
        runFoldersOnHold = runFoldersOnHold
          .filterNot(_.runId == runId),
        unfinishedProcessingOfRunIds = unfinishedProcessingOfRunIds - runId
      )
      val onHold = runFoldersOnHold.filter(_.runId == runId)
      logger.info(
        s"Removed ${onHold.size} runs from hold and re-add the last with id $runId ")

      onHold.lastOption match {
        case None => runRemovedFromUnfinishedDemultiplexing
        case Some(run) =>
          runRemovedFromUnfinishedDemultiplexing
            .addNewRuns(Seq(run))
      }

    }

    def addNewRuns(runs: Seq[RunWithAnalyses]) = {
      val zero = copy(sendToSampleProcessing = Nil,
                      sendToDemultiplexing = Nil,
                      sendToCompleteds = None)
      runs.foldLeft(zero) {
        case (state, run) =>
          if (state.unfinishedProcessingOfRunIds.contains(run.runId)) {
            logger.info(s"Put run on hold: ${run.runId}.")
            state.copy(runFoldersOnHold = state.runFoldersOnHold :+ run)
          } else {
            logger.debug(s"Append to demultiplexables: ${run.runId}.")
            state.copy(
              unfinishedProcessingOfRunIds = state.unfinishedProcessingOfRunIds + run.runId,
              sendToDemultiplexing = state.sendToDemultiplexing :+ run)
          }
      }
    }

    def addNewDemultiplexedSamples(
        samples: (RunWithAnalyses, Seq[DemultiplexedSample]))
      : StateOfUnfinishedSamples = {
      val keys = samples._2.map(getKeysOfDemultiplexedSample)
      logger.debug(s"Add new demultiplexed samples: $keys.")
      copy(unfinished = unfinished ++ keys,
           sendToSampleProcessing = sendToSampleProcessing :+ samples)
    }

    def finish(
        processedSample: Option[SampleResult],
        demultiplexedSample: DemultiplexedSample): StateOfUnfinishedSamples = {
      val keysOfFinishedSample @ (project, _, runId) =
        getKeysOfDemultiplexedSample(demultiplexedSample)

      val remainingUnfinishedSamples =
        unfinished.filterNot(_ == keysOfFinishedSample)
      val runIdsOfRemainingUnfinished = remainingUnfinishedSamples.map(_._3)
      val projectsOfRemainingUnfinished = remainingUnfinishedSamples.map(_._1)

      val runIdIsComplete = !runIdsOfRemainingUnfinished.contains(runId)
      val projectIsComplete = !projectsOfRemainingUnfinished.contains(project)

      val allFinishedSamples = finished ++ processedSample.toSeq

      val samplesOfCompletedRunId =
        if (runIdIsComplete) Some(CompletedRun(allFinishedSamples.filter {
          sampleResult =>
            val runIdOfSampleResult = getKeysOfSampleResult(sampleResult)._3
            runIdOfSampleResult == runId
        }))
        else None

      val samplesOfCompletedProject =
        if (projectIsComplete) Some(CompletedProject(allFinishedSamples.filter {
          sampleResult =>
            val projectOfSampleResult = getKeysOfSampleResult(sampleResult)._1
            projectOfSampleResult == project
        }))
        else None

      val releasableRunWithAnalyses =
        if (!runIdIsComplete) None
        else
          runFoldersOnHold
            .find { runOnHold =>
              val runIdOnHold = runOnHold.runId
              (runIdOnHold: RunId) == (runId: RunId)
            }

      val remainingRunsOnHold = releasableRunWithAnalyses match {
        case None => runFoldersOnHold
        case Some(released) =>
          runFoldersOnHold.filterNot(onHold => onHold == released)
      }

      val newUnfinishedProcessing = {
        val runIdsOfInCompleteRuns =
          if (runIdIsComplete)
            unfinishedProcessingOfRunIds
              .filterNot(_ == keysOfFinishedSample._3)
          else unfinishedProcessingOfRunIds

        val runIdsOfReleasedRunsFromHold = releasableRunWithAnalyses
          .map(_.runId)
          .toSeq

        (runIdsOfInCompleteRuns ++ runIdsOfReleasedRunsFromHold)
      }

      logger.debug(
        s"Accounting the completion of sample processing of $keysOfFinishedSample. Run complete: $runIdIsComplete. Project complete: $projectIsComplete. Remaining unfinished samples ${remainingUnfinishedSamples.size}. Remaining finished samples: ${allFinishedSamples.size}. Unfinished processing: $newUnfinishedProcessing. Runfolders on hold: $remainingRunsOnHold. Released to demux: $releasableRunWithAnalyses")

      StateOfUnfinishedSamples(
        unfinished = remainingUnfinishedSamples,
        finished = allFinishedSamples,
        unfinishedProcessingOfRunIds = newUnfinishedProcessing,
        runFoldersOnHold = remainingRunsOnHold,
        sendToSampleProcessing = Nil,
        sendToDemultiplexing = releasableRunWithAnalyses.toSeq,
        sendToCompleteds =
          Some(Completeds(samplesOfCompletedRunId, samplesOfCompletedProject))
      )
    }

  }

}
