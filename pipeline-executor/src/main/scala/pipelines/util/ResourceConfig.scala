package org.gc.pipelines.util

import com.typesafe.config.ConfigFactory
import tasks._
import org.gc.pipelines.model.{Project, SampleId}

object ResourceConfig {

  def projectAndSampleLabel(project: Project, sample: SampleId) =
    tasks.shared.Labels(
      List("project" -> project,
           "sample" -> sample,
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

  def fastp(implicit tsc: TaskSystemComponents) = parse("fastp")

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

  val picardSamSortRecordPerMegabyteHeap =
    config.getDouble("picardSamSortRecordPerMegabyteHeap")

  val uncompressedBamSizeBytePerRead =
    config.getDouble("uncompressedBamSizeBytePerRead")

}
