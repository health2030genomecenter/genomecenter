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
import akka.stream.{OverflowStrategy, FlowShape}
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
    Source.fromFuture(pipelineState.pastRuns)

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
              duplicate <- pipelineState.contains(run.runId)
              r <- if (duplicate) {
                logger.info(
                  s"Dropping ${run.runId} because it is already processed or under processing. You have to delete this run first, then restart the application.")
                Future.successful(None)
              } else {
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
    .via(demultiplex)
    .via(accountAndProcess)
    .via(processCompletedRunsAndProjects)
    .via(watchTermination)
    .to(Sink.ignore)
    .run

  case class Completeds(runs: CompletedRuns,
                        projects: CompletedProjects,
                        addedUnfinished: Boolean)

  /**
    *
    *                             IN
    *                             |
    *+------------------------------------------------------------------------------------+
    *|                            |Demuxed                                                |
    *|                        +---v--+                                                    |
    *|   +----Demuxed---------+Broadc|                                                    |
    *|   |                    +---+--+             +-----------------------------+        |
    *|   |                        |                |                             |        |
    *|   |  +------------+        |                |     Accounting state        |        |
    *|   |  |            |    +---v---+ Demuxed    |     (scan)                  |        |
    *|   |  |            |    |  Merge+------------>                             |        |
    *|   |  |  Sample    |    +---^---+ Result     |                             |        |
    *|   |  |  Processor |        |                +------------+----------------+        |
    *|   |  |            |        |                             |                         |
    *|   |  |            +-Result-+                             |                         |
    *|   |  +------^-----+                                      |                         |
    *|   |         |                                   +--------v------+                  |
    *|   |        ++-------+                           +    Broadcast  |                  |
    *|   +------->+ Zip    <-Feedback------------------+--------+------+                  |
    *|            +--------+      accounting is done            |                         |
    *|                                                          |                         |
    *+------------------------------------------------------------------------------------+
    *                                                           |
    *                                                           OUT
    */
  def accountAndProcess
    : Flow[Seq[(RunfolderReadyForProcessing, DemultiplexedSample)],
           Completeds,
           _] =
    Flow.fromGraph(GraphDSL.create(accountWorkDone, sampleProcessing)((_, _)) {
      implicit builder => (accountWorkDone, sampleProcessing) =>
        import GraphDSL.Implicits._

        type DM = (RunfolderReadyForProcessing, DemultiplexedSample)

        val broadcastUpstream =
          builder.add(Broadcast[Seq[DM]](2))
        val mergeback =
          builder.add(Merge[Either[SampleResult, Seq[DemultiplexedSample]]](2))

        /* This Zip maintains the lockstep between the sample processor and the accounting
         * It will only send the demultiplexed sample inside the sample processor after
         * the accounting module updated its state
         */
        val zip = builder.add(Zip[Seq[DM], Completeds])

        val broadcastDownstream = builder.add(Broadcast[Completeds](2))

        broadcastUpstream.out(0) ~> zip.in0
        broadcastUpstream.out(1) ~> Flow[Seq[DM]].map(demultiplexed =>
          Right(demultiplexed.map(_._2))) ~> mergeback.in(0)

        zip.out ~> Flow[(Seq[DM], Completeds)]
          .mapConcat(_._1.toList) ~> sampleProcessing ~> Flow[SampleResult]
          .map(Left(_)) ~> mergeback.in(1)

        mergeback.out ~> accountWorkDone ~> broadcastDownstream.in

        broadcastDownstream.out(0) ~> Flow[Completeds]
          .filter(_.addedUnfinished) ~> zip.in1

        FlowShape(broadcastUpstream.in, broadcastDownstream.out(1))
    })

  def demultiplex: Flow[RunfolderReadyForProcessing,
                        Seq[(RunfolderReadyForProcessing, DemultiplexedSample)],
                        _] =
    Flow[RunfolderReadyForProcessing]
      .mapAsync(1000) { run =>
        for {
          samples <- pipeline.demultiplex(run).recover {
            case error =>
              logger.error(s"$pipeline failed on ${run.runId}", error)
              Nil
          }
        } yield samples.map(s => (run, s))
      }

  def accountWorkDone =
    Flow[Either[SampleResult, Seq[DemultiplexedSample]]]
      .scan(StateOfUnfinishedSamples.empty) {
        case (state, Right(demultiplexedSamples)) =>
          logger.info(s"Got $demultiplexedSamples")
          state.addNew(demultiplexedSamples)
        case (state, Left(processedSample)) =>
          logger.info(s"Finishing $processedSample")
          state.finish(processedSample)
      }
      .map(
        state =>
          Completeds(state.completedRuns,
                     state.completedProjects,
                     state.addedUnfinished))

  def processCompletedRunsAndProjects =
    Flow[Completeds]
      .buffer(size = 10000, OverflowStrategy.backpressure)
      .map { case Completeds(runs, projects, _) => (runs, projects) }
      .via(unzipThenMerge(processCompletedRuns, processCompletedProjects))

  def processCompletedRuns =
    Flow[CompletedRuns]
      .via(deduplicate)
      .buffer(1, OverflowStrategy.dropHead)
      .mapConcat(_.groups.toList)
      .mapAsync(1000) { samples =>
        logger.info("Run finished with samples: " + samples.toString)
        pipeline.processCompletedRun(samples).recover {
          case error =>
            val (_, _, runId) = getKeysOfSampleResult(samples.head)
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
    Flow[CompletedProjects]
      .via(deduplicate)
      .buffer(1, OverflowStrategy.dropHead)
      .mapConcat(_.groups.toList)
      .mapAsync(1000) { samples =>
        logger.info("Project finished with samples: " + samples.toString)
        pipeline.processCompletedProject(samples).recover {
          case error =>
            val (project, _, _) = getKeysOfSampleResult(samples.head)
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

    Flow[(RunfolderReadyForProcessing, DemultiplexedSample)]
      .groupBy(maxSubstreams = 10000, {
        case (_, demultiplexedSample) => getSampleId(demultiplexedSample)
      })
      .scanAsync(Option.empty[SampleResult]) {
        case (pastResultsOfThisSample,
              (currentRunConfiguration, currentDemultiplexedSample)) =>
          logger.info(
            s"Processing $currentDemultiplexedSample. Folding into $pastResultsOfThisSample")
          for {
            sampleResult <- pipeline
              .processSample(currentRunConfiguration,
                             pastResultsOfThisSample,
                             currentDemultiplexedSample)
              .map { result =>
                val (project, sampleId, runId) =
                  pipeline.getKeysOfDemultiplexedSample(
                    currentDemultiplexedSample)

                processingFinishedListener ! SampleFinished(project,
                                                            sampleId,
                                                            runId,
                                                            true,
                                                            result)
                result
              }
              .recover {
                case error =>
                  logger.error(
                    s"$pipeline failed on $currentDemultiplexedSample",
                    error)

                  val (project, sampleId, runId) =
                    pipeline.getKeysOfDemultiplexedSample(
                      currentDemultiplexedSample)

                  processingFinishedListener ! SampleFinished(project,
                                                              sampleId,
                                                              runId,
                                                              false,
                                                              None)

                  pastResultsOfThisSample
              }
          } yield sampleResult
      }
      .mergeSubstreams
      .filter(_.isDefined)
      .map(_.get)

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

  case class CompletedProjects(groups: Seq[Seq[SampleResult]])
  case class CompletedRuns(groups: Seq[Seq[SampleResult]])

  object StateOfUnfinishedSamples {
    def empty = StateOfUnfinishedSamples(Set(), Seq(), false)
  }

  case class StateOfUnfinishedSamples(
      unfinished: Set[(Project, SampleId, RunId)],
      finished: Seq[SampleResult],
      addedUnfinished: Boolean) {

    def addNew(samples: Seq[DemultiplexedSample]): StateOfUnfinishedSamples =
      copy(unfinished = unfinished ++ samples.map(sample =>
             getKeysOfDemultiplexedSample(sample)),
           addedUnfinished = true)

    def finish(processedSample: SampleResult): StateOfUnfinishedSamples =
      copy(
        unfinished =
          unfinished.filterNot(_ == getKeysOfSampleResult(processedSample)),
        finished = finished :+ processedSample,
        addedUnfinished = false
      )

    private def completed[T](extractKey: ((Project, SampleId, RunId)) => T) = {
      val keysOfUnfinishedRuns = unfinished.map(extractKey)
      finished
        .groupBy { sampleResult =>
          extractKey(getKeysOfSampleResult(sampleResult))
        }
        .toSeq
        .filterNot {
          case (key, _) =>
            keysOfUnfinishedRuns.contains(key)
        }
        .filter {
          case (key, _) =>
            val keyOfLastFinished =
              extractKey(getKeysOfSampleResult(finished.last))

            key == keyOfLastFinished
        }
        .map { case (_, group) => group }
    }

    def completedRuns: CompletedRuns =
      CompletedRuns(completed(_._3))

    def completedProjects: CompletedProjects =
      CompletedProjects(completed(_._1))
  }

}
