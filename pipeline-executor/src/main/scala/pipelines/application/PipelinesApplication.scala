package org.gc.pipelines.application

import akka.stream.scaladsl.{Source, Sink}
import tasks._
import com.typesafe.scalalogging.StrictLogging
import akka.actor.ActorSystem
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._

import org.gc.pipelines.util.ActorSource

import scala.util.{Success, Failure}

class PipelinesApplication[T](
    eventSource: SequencingCompleteEventSource,
    pipelineState: PipelineState,
    actorSystem: ActorSystem,
    taskSystem: TaskSystem,
    pipeline: Pipeline[T]
)(implicit EC: ExecutionContext)
    extends StrictLogging {

  private val previousUnfinishedRuns =
    Source.fromFuture(pipelineState.incompleteRuns).mapConcat(identity)

  private val futureRuns =
    eventSource.events
      .mapAsync(1) { run =>
        logger.info(s"Got run ${run.runId}")
        pipelineState.completed(run).map(completed => (completed, run))
      }
      .filter {
        case (completed, run) =>
          if (completed) {
            logger.info(
              s"Dropping ${run.runId} because it is already completed.")
          } else {
            logger.debug(s"New run received ${run.runId}")
          }
          !completed
      }
      .map(_._2)
      .mapAsync(1) { run =>
        for {
          _ <- pipelineState.registerNewRun(run)
        } yield run
      }

  private val (processingFinishedListener,
               _processingFinishedSource,
               closeProcessingFinishedSource) =
    ActorSource.make[ProcessingFinished](actorSystem)

  val processingFinishedSource = _processingFinishedSource

  implicit val taskSystemComponents = taskSystem.components
  implicit val materializer = taskSystemComponents.actorMaterializer

  val recoveredState: T = Await.result(pipeline.last, 20 seconds)

  (previousUnfinishedRuns ++ futureRuns)
    .mapAsync(1000) { run =>
      if (!pipeline.canProcess(run))
        Future.successful((run, None))
      else
        for {
          result <- pipeline.execute(run).recover {
            case error =>
              logger.error(s"$pipeline failed on ${run.runId}", error)
              None
          }
        } yield (run, result)

    }
    .mapAsync(1) {
      case (run, result) =>
        val success = result.isDefined
        val saved =
          if (success) {
            logger.info(s"Run ${run.runId} completed successfully.")
            pipelineState.processingFinished(run)
          } else
            Future.successful(())
        saved.map { _ =>
          processingFinishedListener ! ProcessingFinished(run, success)
          logger.debug(s"Run ${run.runId} finished (with or without error).")
          result
        }
    }
    .scan(recoveredState) {
      case (state, Some(runResult)) =>
        pipeline
          .combine(state, runResult)
      case (state, None) =>
        state
    }
    .mapAsync(1) { aggregatedState =>
      pipeline.persist(aggregatedState)
    }
    .mapAsync(1) { aggregatedState =>
      pipeline.aggregateAcrossRuns(aggregatedState).andThen {
        case Success(true) =>
          logger.info("Finished aggregating runs.")
        case Failure(e) =>
          logger.error("Failed aggregating runs.", e)
        case _ =>
          logger.error("Failed aggregating runs.")
      }
    }
    .watchTermination() {
      case (mat, future) =>
        future.onComplete {
          case _ =>
            closeProcessingFinishedSource()
            taskSystem.shutdown
        }
        mat
    }
    .runWith(Sink.ignore)

}
