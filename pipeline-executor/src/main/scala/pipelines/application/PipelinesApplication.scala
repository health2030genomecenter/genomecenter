package org.gc.pipelines.application

import akka.stream.scaladsl.Source
import tasks._
import com.typesafe.scalalogging.StrictLogging
import com.typesafe.config.Config
import akka.actor.ActorSystem

import org.gc.pipelines.util.ActorSource
import org.gc.pipelines.stages.Tasks

class PipelinesApplication(
    eventSource: SequencingCompleteEventSource,
    pipelineState: PipelineState,
    config: Config,
    actorSystem: ActorSystem
) extends StrictLogging {

  import scala.concurrent.ExecutionContext.Implicits.global

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
      for {
        _ <- Tasks.demultiplexing(run)(CPUMemoryRequest(1, 500))
      } yield run
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

  def close() = taskSystem.shutdown

}
