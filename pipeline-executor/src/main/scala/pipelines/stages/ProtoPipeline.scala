package org.gc.pipelines.stages

import scala.concurrent.ExecutionContext
import tasks._
import org.gc.pipelines.application.{Pipeline, RunfolderReadyForProcessing}
import org.gc.pipelines.model._

class ProtoPipeline(implicit EC: ExecutionContext) extends Pipeline {
  def canProcess(r: RunfolderReadyForProcessing) = {
    val sampleSheet = r.sampleSheet.parsed
    sampleSheet.genomeCenterMetadata.contains("automatic") &&
    sampleSheet.genomeCenterMetadata.contains("referenceFasta") &&
    sampleSheet.runId.isDefined
  }
  def execute(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents) = {
    val sampleSheet = r.sampleSheet.parsed

    def fetchReference(sampleSheet: SampleSheet.ParsedData) = {
      val path = sampleSheet.genomeCenterMetadata("referenceFasta")
      val uri = tasks.util.Uri(path)
      SharedFile(uri).map(ReferenceFasta(_))
    }

    for {
      demultiplexed <- Demultiplexing.allLanes(r)(CPUMemoryRequest(1, 500))
      referenceFasta <- fetchReference(sampleSheet)
      perSampleBWAAlignment <- BWAAlignment.allSamples(
        BWAInput(demultiplexed, referenceFasta))(CPUMemoryRequest(1, 500))
    } yield true
  }
}
