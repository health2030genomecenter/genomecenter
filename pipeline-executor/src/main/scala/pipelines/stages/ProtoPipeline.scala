package org.gc.pipelines.stages

import scala.concurrent.{ExecutionContext, Future}
import tasks._
import org.gc.pipelines.application.{Pipeline, RunfolderReadyForProcessing}
import org.gc.pipelines.model._
import org.gc.pipelines.util.parseAsStringList
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

    val sampleSheet = r.sampleSheet.parsed

    def fetchReference(sampleSheet: SampleSheet.ParsedData) = {
      val file = new File(sampleSheet.genomeCenterMetadata("referenceFasta"))
      val fileName = file.getName
      SharedFile(file, fileName).map(ReferenceFasta(_))
    }

    def fetchKnownSitesFiles(sampleSheet: SampleSheet.ParsedData) = {
      val files = parseAsStringList(
        sampleSheet.genomeCenterMetadata("bqsr.knownSites")).right.get

      val fileListWithIndices = files.map { vcfFile =>
        (new File(vcfFile), new File(vcfFile + ".idx"))
      }
      val vcfFilesFuture = fileListWithIndices.map {
        case (vcf, vcfIdx) =>
          for {
            vcf <- SharedFile(vcf, vcf.getName)
            vcfIdx <- SharedFile(vcfIdx, vcfIdx.getName)
          } yield VCF(vcf, Some(vcfIdx))
      }
      Future.sequence(vcfFilesFuture)
    }

    for {
      demultiplexed <- Demultiplexing.allLanes(r)(CPUMemoryRequest(1, 500))
      referenceFasta <- fetchReference(sampleSheet)
      indexedFastaFuture <- BWAAlignment.indexReference(referenceFasta)(
        CPUMemoryRequest(1, 4000))
      perSampleBWAAlignment <- BWAAlignment.allSamples(
        BWAInput(demultiplexed.withoutUndetermined, indexedFastaFuture))(
        CPUMemoryRequest(1, 500))
      knownSites <- fetchKnownSitesFiles(sampleSheet)
      recalibratedBams <- BaseQualityScoreRecalibration.allSamples(
        BQSRInput(perSampleBWAAlignment.bams,
                  indexedFastaFuture,
                  knownSites.toSet)
      )(CPUMemoryRequest(1, 500))
    } yield true

  }

}
