package org.gc.pipelines.application

import tasks._
import com.typesafe.scalalogging.StrictLogging
import akka.actor.ActorSystem
import scala.concurrent.{ExecutionContext, Future}

import org.gc.pipelines.util.ActorSource
import akka.stream.scaladsl.Sink
import akka.stream.ActorMaterializer

class PersistEventSource(
    eventSource: SequencingCompleteEventSource,
    pipelineState: PipelineState
)(implicit EC: ExecutionContext, AS: ActorSystem)
    extends StrictLogging {

  implicit val mat = ActorMaterializer()

  eventSource.commands
    .mapAsync(1) {
      case Delete(runId) =>
        logger.info(s"Deleting $runId")
        pipelineState.invalidated(runId).map(_ => None)
      case Append(run) =>
        logger.debug(s"Got run ${run.runId}")
        val validationErrors = run.validationErrors
        val valid = validationErrors.isEmpty

        if (!valid) {
          logger.info(
            s"$run is not valid (readable?). Validation errors: $validationErrors")
          Future.successful(None)
        } else
          for {
            r <- {
              logger.info(
                s"New run received and persisted. ${run.runId}. You have to reboot the appliation for this to take effect.")
              pipelineState.registered(run)
            }
          } yield r

    }
    .filter(_.isDefined)
    .map(_.get)
    .runWith(Sink.ignore)
}

class SimplePipelinesApplication[DemultiplexedSample, SampleResult](
    pastRuns: Seq[RunfolderReadyForProcessing],
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

  def demultiplex(pastRuns: Seq[RunfolderReadyForProcessing])
    : Future[Seq[(RunfolderReadyForProcessing, DemultiplexedSample)]] =
    Future
      .traverse(pastRuns) { run =>
        pipeline
          .demultiplex(run)
          .recover {
            case error =>
              logger.error(s"$pipeline failed on ${run.runId}", error)
              Nil
          }
          .map { samples =>
            samples.map(s => run -> s)
          }
      }
      .map(_.flatten)

  for {
    demultiplexed <- demultiplex(pastRuns)
    groupedBySample = demultiplexed
      .groupBy {
        case (_, demultiplexedSample) => getSampleId(demultiplexedSample)
      }
      .toSeq
      .map(_._2)

    processedSamples <- Future
      .traverse(groupedBySample) { runsOfSample =>
        Future.sequence {
          (runsOfSample
            .scanLeft(
              Future.successful(List.empty[(SampleResult,
                                            RunfolderReadyForProcessing,
                                            DemultiplexedSample)])) {
              case (pastResultsOfThisSampleFuture,
                    (currentRunConfiguration, currentDemultiplexedSample)) =>
                pastResultsOfThisSampleFuture.flatMap {
                  case pastResultsOfThisSample =>
                    val runsBeforeThis = pastResultsOfThisSample

                    logger.info(
                      s"Processing sample: ${getSampleId(currentDemultiplexedSample)}. RunsBefore: ${runsBeforeThis
                        .map(_._2.runId)}. RunsToReapply: ${currentRunConfiguration.runId}.")

                    val lastSampleResult = pastResultsOfThisSample.lastOption
                      .map {
                        case (sampleResult, _, _) => sampleResult
                      }
                    for {
                      newSampleResult <- foldSample(currentRunConfiguration,
                                                    lastSampleResult,
                                                    currentDemultiplexedSample)
                    } yield
                      newSampleResult match {
                        case None => runsBeforeThis
                        case Some(newSampleResult) =>
                          runsBeforeThis :+ ((newSampleResult,
                                              currentRunConfiguration,
                                              currentDemultiplexedSample))
                      }

                }
            })
            .map(_.filter(_.nonEmpty)
              .map(_.last._1))
        }

      }
      .map(_.flatten)

    processedSamplesByProject = processedSamples
      .groupBy {
        case sampleResult =>
          val (project, _, _) = getKeysOfSampleResult(sampleResult)
          project
      }
      .toSeq
      .map(_._2)

    completedProjects <- Future.traverse(processedSamplesByProject) { samples =>
      assert(samples.map(s => getKeysOfSampleResult(s)._1).distinct.size == 1)

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

  } {
    completedProjects.foreach { project =>
      logger.info(s"Project done. $project")
    }
    closeProcessingFinishedSource()
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

}
