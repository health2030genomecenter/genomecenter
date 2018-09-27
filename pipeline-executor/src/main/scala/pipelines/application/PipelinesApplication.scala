package org.gc.pipelines.application

import akka.stream.scaladsl.Source
import tasks._
import com.typesafe.scalalogging.StrictLogging
import akka.actor.ActorSystem
import scala.concurrent.{ExecutionContext, Future}

import org.gc.pipelines.util.ActorSource

class PipelinesApplication(
    eventSource: SequencingCompleteEventSource,
    pipelineState: PipelineState,
    actorSystem: ActorSystem,
    taskSystem: TaskSystem,
    pipelines: Seq[Pipeline]
)(implicit EC: ExecutionContext)
    extends StrictLogging {

  private val previousUnfinishedRuns =
    Source.fromFuture(pipelineState.incompleteRuns).mapConcat(identity)

  private val futureRuns =
    eventSource.events
      .mapAsync(1) { run =>
        logger.info(s"Got $run")
        pipelineState.completed(run).map(completed => (completed, run))
      }
      .filter {
        case (completed, run) =>
          if (completed) {
            logger.info(s"Dropping $run because it is already completed.")
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

  (previousUnfinishedRuns ++ futureRuns)
    .mapAsync(1) { run =>
      pipelines.find(_.canProcess(run)) match {
        case Some(pipeline) =>
          logger.info(s"Found suitable pipeline for $run")
          for {
            success <- pipeline.execute(run).recover {
              case error =>
                logger.error(s"$pipeline failed on $run", error)
                false
            }
          } yield (run, success)
        case None =>
          logger.info(s"No pipeline to execute $run")
          Future.successful((run, false))

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
    .runForeach {
      case (run, success) =>
        val saved =
          if (success) pipelineState.processingFinished(run)
          else Future.successful(())
        saved.foreach { _ =>
          processingFinishedListener ! ProcessingFinished(run, success)
          logger.info(s"Run $run finished.")
        }
    }

}
