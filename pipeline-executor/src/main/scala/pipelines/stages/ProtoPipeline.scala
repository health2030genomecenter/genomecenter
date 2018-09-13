package org.gc.pipelines.stages

import scala.concurrent.ExecutionContext
import tasks._
import org.gc.pipelines.application.{Pipeline, RunfolderReadyForProcessing}

class ProtoPipeline(implicit EC: ExecutionContext) extends Pipeline {
  def canProcess(r: RunfolderReadyForProcessing) = {
    val sampleSheet = r.sampleSheet.parsed
    sampleSheet.genomeCenterMetadata.contains("automatic") &&
    sampleSheet.runId.isDefined
  }
  def execute(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents) = {
    for {
      demultiplexed <- Demultiplexing.allLanes(r)(CPUMemoryRequest(1, 500))
    } yield true
  }
}
