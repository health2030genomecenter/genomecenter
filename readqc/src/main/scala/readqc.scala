package org.gc.readqc

import htsjdk.samtools.fastq.{FastqReader, FastqRecord}

import java.io._
import scala.collection.JavaConverters._

case class Distribution(
    mean: Double,
    sd: Double,
    min: Double,
    max: Double
)

class DistributionSummary {
  var sum = 0L
  var num = 0L
  var min = Int.MaxValue
  var max = -1

  var M = 0d
  var runningMean = 0d

  def add(i: Int) = {
    num += 1

    /* Welford's algorithm for online one pass variance
     * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_Online_algorithm
     */
    val oldMean = runningMean
    runningMean = runningMean + (i - runningMean) / num
    M = M + (i - runningMean) * (i - oldMean)

    sum += i
    runningMean = sum.toDouble / num
    if (i < min) {
      min = 0
    }
    if (i > max) {
      max = i
    }
  }

  def make =
    Distribution(sum / num.toDouble,
                 math.sqrt(M / (num - 1)),
                 min.toDouble,
                 max.toDouble)

}

case class CycleNumberMetrics(
    cycle: Int,
    baseQ: Distribution,
    numberOfNs: Long
)

case class Metrics(
    baseQ: Distribution,
    cycles: Seq[CycleNumberMetrics],
    numberOfDistinct13Mers: Int,
    readNumber: Long,
    gcFraction: Double
)

object Metrics {
  import io.circe.generic.auto._
  import io.circe.generic.semiauto._
  import io.circe._
  implicit val encoder: Encoder[Metrics] =
    deriveEncoder[Metrics]
  implicit val decoder: Decoder[Metrics] =
    deriveDecoder[Metrics]

}

trait Read {
  def bases: Array[Byte]
  def baseQ: Array[Byte]
}

object ReadQC {

  def processHtsJdkRecords(htsjdkRecords: Iterator[FastqRecord]): Metrics = {
    process(htsjdkRecords.map { fq =>
      new Read {
        def bases = fq.getReadBases
        def baseQ = fq.getBaseQualities
      }
    })
  }

  def process(files: Seq[File]): Metrics = {
    val fqReader = files.map(file => new FastqReader(file))
    try {
      val it = fqReader.iterator.flatMap(_.iterator.asScala).map { fq =>
        new Read {
          def bases = fq.getReadBases
          def baseQ = fq.getBaseQualities
        }
      }
      process(it)
    } finally {
      fqReader.foreach(_.close)

    }
  }

  def process(reads: Iterator[Read]): Metrics = {
    val totalBaseQDistribution = new DistributionSummary
    val cycleBaseQDistributions =
      Array.fill[DistributionSummary](2000)(new DistributionSummary)
    val cycleBaseN = Array.fill(2000)(0L)
    var maxCycle = -1
    var readNumber = 0L

    // This is the maximum which fits into a plain java array
    val kmerLength = 13

    val kmers = Array.ofDim[Byte](math.pow(5, kmerLength.toDouble).toInt)
    var totalBases = 0L
    var gcBases = 0L

    def addCycleBaseN(i: Int) = {
      cycleBaseN(i) += 1
    }

    reads.foreach { read =>
      val bases = read.bases
      val qual = read.baseQ

      if (bases.length >= kmerLength) {
        kmers(stringToInt(bases, max = kmerLength)) = 1
      }
      {
        var cycleIdx = 0
        qual.foreach { baseQ =>
          val asInt = baseQ.toInt
          totalBaseQDistribution.add(asInt)
          cycleBaseQDistributions(cycleIdx).add(asInt)
          cycleIdx += 1
        }
      }

      {
        var cycleIdx = 0

        var i = 0
        while (i < bases.length) {
          if (maxCycle < i) {
            maxCycle = i
          }
          val base = bases(i)
          if (base == 'G' || base == 'C') {
            gcBases += 1
          }
          if (base == 'N') {
            addCycleBaseN(cycleIdx)
          }
          cycleIdx += 1
          totalBases += 1
          i += 1
        }
      }

      readNumber += 1

    }

    val gcFraction = gcBases.toDouble / totalBases

    val cycles = (cycleBaseQDistributions.take(maxCycle + 1) zip cycleBaseN
      .take(maxCycle + 1) zipWithIndex) map {
      case ((dist, baseN), idx) =>
        CycleNumberMetrics(
          idx + 1,
          dist.make,
          baseN
        )
    }

    Metrics(
      baseQ = totalBaseQDistribution.make,
      cycles = cycles,
      numberOfDistinct13Mers = kmers.count(_ > 0),
      readNumber = readNumber,
      gcFraction = gcFraction
    )

  }

  def stringToInt(s: Array[Byte], max: Int): Int = {
    var sum = 0
    var i = 0
    while (i < max) {
      val c = s(i) match {
        case 'A' => 0
        case 'C' => 1
        case 'G' => 2
        case 'T' => 3
        case _   => 4
      }
      sum = 5 * sum + c
      i += 1
    }
    sum
  }
}
