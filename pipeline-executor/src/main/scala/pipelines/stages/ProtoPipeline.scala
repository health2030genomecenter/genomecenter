package org.gc.pipelines.stages

import scala.concurrent.ExecutionContext
import tasks._
import org.gc.pipelines.application.{Pipeline, RunfolderReadyForProcessing}
import org.gc.pipelines.model._
import java.io.File

class ProtoPipeline(implicit EC: ExecutionContext) extends Pipeline {
  def canProcess(r: RunfolderReadyForProcessing) = {
    val sampleSheet = r.sampleSheet.parsed
    sampleSheet.genomeCenterMetadata.contains("automatic") &&
    sampleSheet.genomeCenterMetadata.contains("referenceFasta") &&
    sampleSheet.runId.isDefined
  }

  def execute(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents) = {

    tsc.withFilePrefix(Seq(r.runId)) { implicit tsc =>
      val sampleSheet = r.sampleSheet.parsed

      def fetchReference(sampleSheet: SampleSheet.ParsedData) = {
        val file = new File(sampleSheet.genomeCenterMetadata("referenceFasta"))
        val fileName = file.getName
        SharedFile(file, fileName).map(ReferenceFasta(_))
      }

      for {
        demultiplexed <- Demultiplexing.allLanes(r)(CPUMemoryRequest(1, 500))
        referenceFasta <- fetchReference(sampleSheet)
        perSampleBWAAlignment <- BWAAlignment.allSamples(
          BWAInput(demultiplexed.withoutUndetermined, referenceFasta))(
          CPUMemoryRequest(1, 500))
      } yield true
    }

  }

}
