package org.gc.pipelines.util

import com.typesafe.config.ConfigFactory
import tasks._

object ResourceConfig {
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
