package org.gc.pipelines.application

import tasks._
import com.typesafe.scalalogging.StrictLogging
import akka.actor.ActorSystem
import scala.concurrent.{ExecutionContext, Future}

import org.gc.pipelines.util.{ActorSource, traverseAll}
import org.gc.pipelines.model.{Project, SampleId}
import akka.stream.scaladsl.Sink
import akka.stream.ActorMaterializer

class PersistCommandSource(
    commandSource: CommandSource,
    pipelineState: PipelineState
)(implicit EC: ExecutionContext, AS: ActorSystem)
    extends StrictLogging {

  implicit val mat = ActorMaterializer()

  val persistCommandsFunction =
    PipelineStreamProcessor.persistCommands(pipelineState)

  commandSource.commands
    .mapAsync(1)(persistCommandsFunction)
    .runWith(Sink.ignore)
}

class PipelineBatchProcessor[DemultiplexedSample, SampleResult, Delivery](
    pastRuns: Seq[RunWithAnalyses],
    actorSystem: ActorSystem,
    taskSystem: TaskSystem,
    pipeline: Pipeline[DemultiplexedSample, SampleResult, Delivery],
    blacklist: Set[(Project, SampleId)]
)(implicit EC: ExecutionContext)
    extends StrictLogging
    with WithFinished {

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

  def demultiplex(pastRuns: Seq[RunWithAnalyses])
    : Future[Seq[(RunWithAnalyses, DemultiplexedSample)]] = {
    Future
      .traverse(pastRuns) { runWithAnalyses =>
        pipeline
          .demultiplex(runWithAnalyses.run)
          .recover {
            case error =>
              logger.error(s"$pipeline failed on ${runWithAnalyses.run.runId}",
                           error)
              Nil
          }
          .map { samples =>
            val filteredSamples = samples.filterNot { sample =>
              val (project, sampleId, _) = getKeysOfDemultiplexedSample(sample)
              blacklist.contains((project, sampleId))
            }
            filteredSamples.map(s => runWithAnalyses -> s)
          }
      }
      .map(_.flatten)
  }

  val finished = for {
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
            .scanLeft(Future.successful(List
              .empty[(SampleResult, RunWithAnalyses, DemultiplexedSample)])) {
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
                      newSampleResult <- PipelineStreamProcessor.foldSample(
                        pipeline,
                        processingFinishedListener,
                        currentRunConfiguration,
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

    completedProjects <- traverseAll(processedSamplesByProject) { samples =>
      assert(samples.map(s => getKeysOfSampleResult(s)._1).distinct.size == 1)

      PipelineStreamProcessor.processSamplesOfCompletedProject(pipeline,
                                                               samples)

    }

  } yield {
    completedProjects.foreach { project =>
      logger.info(s"Project done. $project")
    }
    closeProcessingFinishedSource()
  }

}
