package org.gc.pipelines

import akka.stream.scaladsl.Source
import scala.concurrent.Future
import scala.concurrent.duration._
import tasks._
import tasks.circesupport._
import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import com.typesafe.scalalogging.StrictLogging
import com.typesafe.config.{Config, ConfigFactory}
import akka.actor.ActorSystem

case class SampleSheet(content: String)

object SampleSheet {
  implicit val encoder: Encoder[SampleSheet] =
    deriveEncoder[SampleSheet]
  implicit val decoder: Decoder[SampleSheet] =
    deriveDecoder[SampleSheet]
}

case class RunfolderReadyForProcessing(runId: String,
                                       sampleSheet: SampleSheet,
                                       runFolderPath: String)

case class ProcessingFinished(run: RunfolderReadyForProcessing)

object RunfolderReadyForProcessing {
  implicit val encoder: Encoder[RunfolderReadyForProcessing] =
    deriveEncoder[RunfolderReadyForProcessing]
  implicit val decoder: Decoder[RunfolderReadyForProcessing] =
    deriveDecoder[RunfolderReadyForProcessing]
}

trait SequencingCompleteEventSource {
  def events: Source[RunfolderReadyForProcessing, _]
}

class FakeSequencingCompleteEventSource
    extends SequencingCompleteEventSource
    with StrictLogging {
  def events =
    Source
      .tick(
        1 seconds,
        2 seconds,
        RunfolderReadyForProcessing("fake", SampleSheet("fake"), "fakePath"))
      .take(3)
}

trait PipelineState {
  def processingFinished(r: RunfolderReadyForProcessing): Future[Unit]
  def incompleteRuns: Future[List[RunfolderReadyForProcessing]]
  def registerNewRun(r: RunfolderReadyForProcessing): Future[Unit]
}

class InMemoryPipelineState extends PipelineState {
  private var state = List[RunfolderReadyForProcessing]()
  def incompleteRuns = Future.successful(state)
  def registerNewRun(r: RunfolderReadyForProcessing) = synchronized {
    state = r :: state
    Future.successful(())
  }
  def processingFinished(r: RunfolderReadyForProcessing) = synchronized {
    state = state.filterNot(_ == r)
    Future.successful(())
  }

}

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

object Tasks {

  val demultiplexing =
    AsyncTask[RunfolderReadyForProcessing, Int]("demultiplexing", 1) {
      input => implicit computationEnvironment =>
        log.info(s"Pretending that run $input is being processed..")
        Future.successful(1)
    }

}

object Main extends App with StrictLogging {
  logger.info("Main thread started.")
  val config = ConfigFactory.load
  val eventSource = new FakeSequencingCompleteEventSource
  val pipelineState = new InMemoryPipelineState
  val actorSystem = ActorSystem("Main")
  val app =
    new PipelinesApplication(eventSource, pipelineState, config, actorSystem)
  logger.info("Main thread will stop.")
}
