package org.gc.pipelines.application

import akka.stream.scaladsl.Source
import tasks._
import com.typesafe.scalalogging.StrictLogging
import com.typesafe.config.Config
import akka.actor.ActorSystem
import scala.concurrent.{ExecutionContext, Future}

import org.gc.pipelines.util.ActorSource

class PipelinesApplication(
    eventSource: SequencingCompleteEventSource,
    pipelineState: PipelineState,
    config: Config,
    actorSystem: ActorSystem,
    pipelines: Seq[Pipeline]
)(implicit EC: ExecutionContext)
    extends StrictLogging {

  private val previousUnfinishedRuns =
    Source.fromFuture(pipelineState.incompleteRuns).mapConcat(identity)

  private val futureRuns = eventSource.events.mapAsync(1) { run =>
    for {
      _ <- pipelineState.registerNewRun(run)
    } yield run
  }

  private val (processingFinishedListener,
               _processingFinishedSource,
               closeProcessingFinishedSource) =
    ActorSource.make[ProcessingFinished](actorSystem)

  val processingFinishedSource = _processingFinishedSource

  private val taskSystem = defaultTaskSystem(Some(config))
  implicit val taskSystemComponents = taskSystem.components
  implicit val materializer = taskSystemComponents.actorMaterializer

  (previousUnfinishedRuns ++ futureRuns)
    .mapAsync(1) { run =>
      pipelines.find(_.canProcess(run)) match {
        case Some(pipeline) =>
          logger.info(s"Found suitable pipeline for $run")
          for {
            _ <- pipeline.execute(run)
          } yield run
        case None =>
          logger.info(s"No pipeline to execute $run")
          Future.successful(run)

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
    .runForeach { run =>
      pipelineState.processingFinished(run).foreach { _ =>
        processingFinishedListener ! ProcessingFinished(run)
        logger.info(s"Run $run finished.")
      }
    }

}
