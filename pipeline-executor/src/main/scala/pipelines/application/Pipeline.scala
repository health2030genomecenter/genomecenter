package org.gc.pipelines.application

import scala.concurrent.Future
import tasks._

import org.gc.pipelines.model.{Project, SampleId, RunId}

trait Pipeline[DemultiplexedSample, SampleResult, DeliverableList] {
  def canProcess(r: RunfolderReadyForProcessing): Boolean
  def demultiplex(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents): Future[Seq[DemultiplexedSample]]

  def getKeysOfDemultiplexedSample(
      d: DemultiplexedSample): (Project, SampleId, RunId)
  def getKeysOfSampleResult(d: SampleResult): (Project, SampleId, RunId)

  def processCompletedRun(samples: Seq[SampleResult])(
      implicit tsc: TaskSystemComponents): Future[(RunId, Boolean)]
  def processCompletedProject(samples: Seq[SampleResult])(
      implicit tsc: TaskSystemComponents)
    : Future[(Project, Boolean, Option[DeliverableList])]

  def processSample(runConfiguration: RunfolderReadyForProcessing,
                    analysisAssignments: AnalysisAssignments,
                    pastSampleResult: Option[SampleResult],
                    demultiplexedSample: DemultiplexedSample)(
      implicit tsc: TaskSystemComponents): Future[Option[SampleResult]]

}
