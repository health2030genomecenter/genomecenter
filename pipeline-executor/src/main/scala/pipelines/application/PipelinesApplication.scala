package org.gc.pipelines.application

import akka.stream.scaladsl.{
  Source,
  Sink,
  Flow,
  GraphDSL,
  Broadcast,
  Merge,
  Zip
}
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
case class ProjectFinished(project: Project, success: Boolean)
case class SampleFinished[T](project: Project,
                             sample: SampleId,
                             runId: RunId,
                             success: Boolean,
                             result: Option[T])

class PipelinesApplication[DemultiplexedSample, SampleResult](
    eventSource: SequencingCompleteEventSource,
    pipelineState: PipelineState,
    actorSystem: ActorSystem,
    taskSystem: TaskSystem,
    pipeline: Pipeline[DemultiplexedSample, SampleResult]
)(implicit EC: ExecutionContext)
    extends StrictLogging {

  import pipeline.getKeysOfDemultiplexedSample
  import pipeline.getKeysOfSampleResult

  private def getSampleId(s: DemultiplexedSample) = {
    val (project, sampleId, _) = getKeysOfDemultiplexedSample(s)
    (project, sampleId)
  }

  private val previousUnfinishedRuns =
    Source.fromFuture(pipelineState.pastRuns).map { runs =>
      runs.foreach { run =>
        logger.info(s"Recovered past runs ${run.runId}")
      }
      runs
    }

  private val futureRuns =
    eventSource.commands
      .mapAsync(1) {
        case Delete(runId) =>
          logger.info(s"Deleting $runId")
          pipelineState.invalidated(runId).map(_ => None)
        case Append(run) =>
          logger.info(s"Got run ${run.runId}")
          val valid = run.isValid

          if (!valid) {
            logger.info(s"$run is not valid (readable?)")
            Future.successful(None)
          } else
            for {
              r <- {
                logger.debug(s"New run received ${run.runId}")
                pipelineState.registered(run)
              }
            } yield r

      }
      .filter(_.isDefined)
      .map(_.get)

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

  implicit val taskSystemComponents = taskSystem.components
  implicit val materializer = taskSystemComponents.actorMaterializer

  (previousUnfinishedRuns.mapConcat(identity) ++ futureRuns)
    .filter(pipeline.canProcess)
    .via(accountAndProcess)
    .via(processCompletedRunsAndProjects)
    .via(watchTermination)
    .to(Sink.ignore)
    .run

  case class Completeds(runs: CompletedRun,
                        projects: CompletedProject,
                        addedUnfinished: Boolean)

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
   *|                        +------+      |                                             |
   *|   +----Demuxed---------<Broadc<----- | ---[Demux]-Raw-<--+                         |
   *|   |                    +---v--+      |      +------------^----------------+        |
   *|   |                        |  .______.      |                             |        |
   *|   |  +------------+        |  |             |     accountWorkDone         |        |
   *|   |  |            |    +---v--v+ Demuxed    |                             |        |
   *|   |  |            |    | merge >------------> Accumulates state of runs   |        |
   *|   |  |  Sample    |    +---^---+ Result     |   and samples               |        |
   *|   |  |  Processor |        |                +--v---------v----------------+        |
   *|   |  |            |        |                   |         |                         |
   *|   |  |            >-Result-+                   |         |                         |
   *|   |  +------^-----+                            |         |                         |
   *|   |         |                                  |         |completeds               |
   *|   |        +^-------+                          |         |                         |
   *|   +------->+ zip    <--Feedback----------------.         |                         |
   *|            +--------+  accounting of demultiplexed       |                         |
   *|                        sample is done                    |                         |
   *+------------------------------------------------------------------------------------+
   *                                                           |
   *                                                           OUT [Completeds]
   */
  def accountAndProcess: Flow[RunfolderReadyForProcessing, Completeds, _] =
    Flow.fromGraph(
      GraphDSL
        .create(accountWorkDone, demultiplex, sampleProcessing)((_, _, _)) {
          implicit builder =>
            (accountWorkDone, demultiplex, sampleProcessing) =>
              import GraphDSL.Implicits._

              type DM = (RunfolderReadyForProcessing, Seq[DemultiplexedSample])

              val broadcastDemultiplexed =
                builder.add(Broadcast[DM](2))
              val merge =
                builder.add(Merge[Stage](3))

              /* This Zip maintains the lockstep between the sample processor and the accounting
               * It will only send the demultiplexed sample inside the sample processor after
               * the accounting module updated its state
               */
              val zip = builder.add(Zip[DM, Unit])

              val mapToRaw =
                builder.add(Flow[RunfolderReadyForProcessing].map(runFolder =>
                  Raw(runFolder)))
              mapToRaw.out ~> merge.in(2)

              broadcastDemultiplexed.out(0) ~> zip.in0

              broadcastDemultiplexed.out(1) ~> Flow[DM].map {
                case (run, demultiplexedSamples) =>
                  Demultiplexed(run, demultiplexedSamples)
              } ~> merge.in(0)

              zip.out ~> Flow[(DM, Unit)]
                .mapConcat {
                  case ((run, samples), _) =>
                    samples.map(s => (run, s)).toList
                } ~> sampleProcessing ~> Flow[SampleResult]
                .map(ProcessedSample(_)) ~> merge.in(1)

              merge.out ~> accountWorkDone.in

              accountWorkDone.out0 ~> zip.in1
              accountWorkDone.out2 ~> demultiplex ~> broadcastDemultiplexed.in

              FlowShape(mapToRaw.in, accountWorkDone.out1)
        })

  def demultiplex: Flow[RunfolderReadyForProcessing,
                        (RunfolderReadyForProcessing, Seq[DemultiplexedSample]),
                        _] =
    Flow[RunfolderReadyForProcessing]
      .mapAsync(1000) { run =>
        logger.info(s"Demultiplex run ${run.runId}")
        for {
          samples <- pipeline.demultiplex(run).recover {
            case error =>
              logger.error(s"$pipeline failed on ${run.runId}", error)
              Nil
          }
        } yield {
          logger.info(
            s"Demultiplexing of ${run.runId} with ${samples.size} done.")
          run -> samples
        }
      }

  def accountWorkDone
    : Graph[FanOutShape3[Stage, Unit, Completeds, RunfolderReadyForProcessing],
            NotUsed] = {

    val state = Flow[Stage]
      .scan(StateOfUnfinishedSamples.empty) {
        case (state, Demultiplexed(_, demultiplexedSamples))
            if demultiplexedSamples.nonEmpty =>
          val sampleIds = demultiplexedSamples.map(ds => getSampleId(ds))
          logger.info(s"Got demultiplexed samples: ${sampleIds.mkString(", ")}")
          state.addNewDemultiplexedSamples(demultiplexedSamples)

        case (state, Demultiplexed(run, _)) =>
          logger.info(s"Demultiplexed 0 samples.")
          state.removeFromUnfinishedDemultiplexing(run.runId)

        case (state, ProcessedSample(processedSample)) =>
          logger.info(
            s"Finishing ${pipeline.getKeysOfSampleResult(processedSample)}")
          state.finish(processedSample)

        case (state, Raw(runFolder)) =>
          logger.info(s"Got new run ${runFolder.runId}")
          state.addNewRun(runFolder)
      }

    GraphDSL
      .create(state) { implicit builder => stateScan =>
        import GraphDSL.Implicits._

        val broadcast =
          builder.add(Broadcast[StateOfUnfinishedSamples](3))

        val completeds = builder.add(
          Flow[StateOfUnfinishedSamples].map(
            state =>
              Completeds(state.completedRun,
                         state.completedProject,
                         state.registeredDemultiplexedSamples)))

        val addedUnfinished = builder.add(
          Flow[StateOfUnfinishedSamples]
            .filter(_.registeredDemultiplexedSamples)
            .map(_ => ())
        )

        val rawRunFolders = builder.add(
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
        broadcast.out(1) ~> addedUnfinished.in
        broadcast.out(2) ~> rawRunFolders.in

        new FanOutShape3(stateScan.in,
                         addedUnfinished.out,
                         completeds.out,
                         rawRunFolders.out)
      }

  }

  def processCompletedRunsAndProjects =
    Flow[Completeds]
      .buffer(size = 10000, OverflowStrategy.backpressure)
      .map { case Completeds(runs, projects, _) => (runs, projects) }
      .via(unzipThenMerge(processCompletedRuns, processCompletedProjects))

  def processCompletedRuns =
    Flow[CompletedRun]
      .filter(_.samples.nonEmpty)
      .via(deduplicate)
      .buffer(1, OverflowStrategy.dropHead)
      .mapAsync(1000) {
        case CompletedRun(samples) =>
          val (_, _, runId) = getKeysOfSampleResult(samples.head)
          logger.info(s"Run finished with ${samples.size} samples: $runId")
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

  def processCompletedProjects =
    Flow[CompletedProject]
      .filter(_.samples.nonEmpty)
      .via(deduplicate)
      // A buffer of size one with DropHead overflow strategy ensures
      // that the stream is always pulled and the latest element is processed.
      // If there is a newer element, then we want to drop the previously buffered elements
      // because we care for the latest
      .buffer(1, OverflowStrategy.dropHead)
      .mapAsync(1000) {
        case CompletedProject(samples) =>
          val (project, _, _) = getKeysOfSampleResult(samples.head)

          val lastRunOfEachSample = samples.zipWithIndex
            .groupBy {
              case (sample, _) =>
                val (_, sampleId, _) = getKeysOfSampleResult(sample)
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
            s"Project finished with ${lastRunOfEachSample.size} samples: $project .")
          pipeline.processCompletedProject(lastRunOfEachSample).recover {
            case error =>
              logger.error(
                s"$pipeline failed on $project while processing completed project",
                error)
              (project, false)
          }
      }
      .map {
        case (project, success) =>
          processingFinishedListener ! ProjectFinished(project, success)
          logger.debug(
            s"Processing of completed project $project finished (with or without error). Success: $success.")
          project
      }

  def sampleProcessing: Flow[(RunfolderReadyForProcessing, DemultiplexedSample),
                             SampleResult,
                             _] = {

    val maxConcurrentlyProcessedSamples = 10000

    Flow[(RunfolderReadyForProcessing, DemultiplexedSample)]
      .groupBy(maxSubstreams = maxConcurrentlyProcessedSamples, {
        case (_, demultiplexedSample) => getSampleId(demultiplexedSample)
      })
      .scanAsync(
        List.empty[(SampleResult,
                    RunfolderReadyForProcessing,
                    DemultiplexedSample)]) {
        case (pastResultsOfThisSample,
              (currentRunConfiguration, currentDemultiplexedSample)) =>
          val (runsBeforeThis, runsAfterInclusive) =
            pastResultsOfThisSample.span {
              case (_, pastRunFolder, _) =>
                pastRunFolder.runId != currentRunConfiguration.runId
            }

          val runsAfterThis = runsAfterInclusive.drop(1).map {
            case (_, runFolder, demultiplexedSamples) =>
              (runFolder, demultiplexedSamples)
          }

          val runsToReApply = ((currentRunConfiguration,
                                currentDemultiplexedSample)) +: runsAfterThis

          logger.info(
            s"Processing sample: ${getSampleId(currentDemultiplexedSample)}. RunsBefore: ${runsBeforeThis
              .map(_._2.runId)}. RunsToReapply: ${runsToReApply.map(_._1.runId)}.")

          runsToReApply.foldLeft(Future.successful(runsBeforeThis)) {
            case (pastIntermediateResults,
                  (currentRunConfiguration, currentDemultiplexedSample)) =>
              for {
                pastIntermediateResults <- pastIntermediateResults

                lastSampleResult = pastIntermediateResults.lastOption.map {
                  case (sampleResult, _, _) => sampleResult
                }

                newSampleResult <- foldSample(currentRunConfiguration,
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

  private def foldSample(currentRunConfiguration: RunfolderReadyForProcessing,
                         pastResultsOfThisSample: Option[SampleResult],
                         currentDemultiplexedSample: DemultiplexedSample) =
    pipeline
      .processSample(currentRunConfiguration,
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
  case class Raw(run: RunfolderReadyForProcessing) extends Stage
  case class Demultiplexed(run: RunfolderReadyForProcessing,
                           demultiplexed: Seq[DemultiplexedSample])
      extends Stage
  case class ProcessedSample(sampleResult: SampleResult) extends Stage

  object StateOfUnfinishedSamples {
    def empty = StateOfUnfinishedSamples(Nil, Set(), Set(), Seq(), false, Nil)
  }

  case class StateOfUnfinishedSamples(
      runFoldersOnHold: Seq[RunfolderReadyForProcessing],
      unfinishedDemultiplexingOfRunIds: Set[RunId],
      unfinished: Set[(Project, SampleId, RunId)],
      finished: Seq[SampleResult],
      registeredDemultiplexedSamples: Boolean,
      sendToDemultiplexing: Seq[RunfolderReadyForProcessing])
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
        registeredDemultiplexedSamples = true,
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
            .copy(registeredDemultiplexedSamples = true)
      }

    }

    def addNewRun(run: RunfolderReadyForProcessing) =
      if (unfinishedDemultiplexingOfRunIds.contains(run.runId)) {
        logger.info(s"Put run on hold: ${run.runId}.")
        copy(runFoldersOnHold = runFoldersOnHold :+ run,
             registeredDemultiplexedSamples = false,
             sendToDemultiplexing = Nil)
      } else {
        logger.debug(s"Set state to send to demultiplexing: ${run.runId}.")
        removeSamplesOfCompletedRuns.copy(
          unfinishedDemultiplexingOfRunIds = unfinishedDemultiplexingOfRunIds + run.runId,
          registeredDemultiplexedSamples = false,
          sendToDemultiplexing = List(run))
      }

    def addNewDemultiplexedSamples(
        samples: Seq[DemultiplexedSample]): StateOfUnfinishedSamples = {
      val keys = samples.map(getKeysOfDemultiplexedSample)
      logger.debug(s"Add new demultiplexed samples: $keys.")
      copy(unfinished = unfinished ++ keys,
           registeredDemultiplexedSamples = true,
           sendToDemultiplexing = Nil)
    }

    def finish(processedSample: SampleResult): StateOfUnfinishedSamples = {
      val keysOfFinishedSample = getKeysOfSampleResult(processedSample)
      logger.debug(s"Finishing $keysOfFinishedSample.")
      copy(
        unfinished = unfinished.filterNot(_ == keysOfFinishedSample),
        finished = finished :+ processedSample,
        unfinishedDemultiplexingOfRunIds = unfinishedDemultiplexingOfRunIds
          .filterNot(_ == keysOfFinishedSample._3),
        registeredDemultiplexedSamples = false,
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
            s"Finished $keyOfLastFinishedSample . Remaining unfinished: $keysOfUnfinishedSamples")
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
