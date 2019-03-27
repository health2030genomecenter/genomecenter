package org.gc.pipelines.util

import com.typesafe.config.ConfigFactory
import tasks._
import org.gc.pipelines.model.{Project, SampleId, AnalysisId, RunId}
import org.gc.pipelines.stages.PerSamplePerRunFastQ
import scala.collection.JavaConverters._

object ResourceConfig {

  def projectAndSampleLabel(project: Project,
                            sample: SampleId,
                            analysisId: AnalysisId,
                            runId: RunId) =
    tasks.shared.Labels(
      List("project" -> project,
           "sample" -> sample,
           "analysisId" -> analysisId,
           "run" -> runId,
           "uuid" -> java.util.UUID.randomUUID.toString))

  def projectLabel(projects: Project*) =
    tasks.shared.Labels(projects.toList.map(p => "project" -> p))

  val config = ConfigFactory.load.getConfig("gc.resourceRequests")

  private def parse(path: String)(implicit tsc: TaskSystemComponents) = {
    val subtree = config.getConfig(path)
    if (subtree.hasPath("cpumax")) {
      ResourceRequest((subtree.getInt("cpu"), subtree.getInt("cpumax")),
                      subtree.getInt("ram"),
                      subtree.getInt("scratch"))
    } else
      ResourceRequest(subtree.getInt("cpu"),
                      subtree.getInt("ram"),
                      subtree.getInt("scratch"))
  }

  def bcl2fastq(implicit tsc: TaskSystemComponents) = parse("bcl2fastq")

  def qtlToolsQuantification(implicit tsc: TaskSystemComponents) =
    parse("qtlToolsQuantification")

  def fastp(fastqs: PerSamplePerRunFastQ)(
      implicit tsc: TaskSystemComponents) = {
    val originalRequest = parse("fastp")
    val totalReads = fastqs.lanes.toSeq.toList
      .flatMap(lane => List(lane.read1, lane.read2) ++ lane.umi.toList)
      .map(_.numberOfReads.toDouble)
      .sum
    originalRequest.copy(
      cpuMemoryRequest = originalRequest.cpuMemoryRequest.copy(scratch =
        (uncompressedBamSizeBytePerRead * totalReads * 1.1 * 1E-6).toInt))
  }

  def bwa(implicit tsc: TaskSystemComponents) = parse("bwa")

  def sortBam(implicit tsc: TaskSystemComponents) = parse("sortBam")

  def readQC(implicit tsc: TaskSystemComponents) = parse("readQC")

  def haplotypeCaller(implicit tsc: TaskSystemComponents) =
    parse("haplotypeCaller")

  def genotypeGvcfs(implicit tsc: TaskSystemComponents) =
    parse("genotypeGvcfs")

  def picardMergeAndMarkDuplicates(implicit tsc: TaskSystemComponents) =
    parse("picardMergeAndMarkDuplicates")

  def indexReference(implicit tsc: TaskSystemComponents) =
    parse("indexReference")

  def vqsrTrainIndel(implicit tsc: TaskSystemComponents) =
    parse("vqsrTrainIndel")
  def vqsrTrainSnp(implicit tsc: TaskSystemComponents) =
    parse("vqsrTrainSnp")
  def vqsrApply(implicit tsc: TaskSystemComponents) =
    parse("vqsrApply")

  def createStarIndex(implicit tsc: TaskSystemComponents) =
    parse("createStarIndex")
  def starAlignment(implicit tsc: TaskSystemComponents) =
    parse("starAlignment")

  def trainBqsr(implicit tsc: TaskSystemComponents) =
    parse("trainBqsr")

  def applyBqsr(implicit tsc: TaskSystemComponents) =
    parse("applyBqsr")

  def minimal(implicit tsc: TaskSystemComponents) =
    parse("minimal")

  def collectHSMetrics(implicit tsc: TaskSystemComponents) =
    parse("collectHSMetrics")

  val picardSamSortRecordPerMegabyteHeap =
    config.getDouble("picardSamSortRecordPerMegabyteHeap")

  val uncompressedBamSizeBytePerRead =
    config.getDouble("uncompressedBamSizeBytePerRead")

  val compressedFastQSizeBytePerRead =
    config.getDouble("compressedFastQSizeBytePerRead")

  val genotypeGvcfScratchSpaceMegabytePerSample =
    config.getDouble("genotypeGvcfScratchSpaceMegabytePerSample")

  val projectPriorities: Map[Project, Int] =
    if (config.hasPath("projectPriorities"))
      config
        .getConfigList("projectPriorities")
        .asScala
        .map { c =>
          val project = c.getString("project")
          val value = c.getInt("priority")
          (Project(project), value)
        }
        .toMap
    else Map.empty

  def projectPriority(project: Project) =
    projectPriorities.get(project).getOrElse(1000000)

  val bwaMaxReadsPerChunk = config.getLong("bwaMaxReadsPerChunk")

}
