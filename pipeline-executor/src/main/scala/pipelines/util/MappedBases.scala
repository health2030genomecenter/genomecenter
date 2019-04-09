package org.gc.pipelines.util

import scala.collection.JavaConverters._
import java.io.File

object MappedBases {

  def countBases(file: File,
                 minimumMapQ: Int,
                 minimumReadLength: Int,
                 bedFile: Option[String]) = {

    val intervaltrees = bedFile.map { bedFile =>
      val intervals = fileutils
        .openSource(bedFile)(
          _.getLines
            .filterNot(_.startsWith("#"))
            .map { line =>
              val spl = line.split("\t")
              spl(0) -> intervaltree.IntervalWithPayLoad(spl(1).toInt,
                                                         spl(2).toInt,
                                                         ())
            }
            .toList
            .sortBy(_._2.from)
            .reverse
        )

      intervaltree.IntervalTree.intervalForest(intervals.iterator)
    }

    def inTarget(contig: String, bp: Int) = intervaltrees match {
      case None => true
      case Some(trees) =>
        trees
          .get(contig)
          .map(
            tree =>
              intervaltree.IntervalTree
                .lookup(intervaltree.IntervalWithPayLoad(bp, bp, ()), tree)
                .nonEmpty)
          .getOrElse(false)
    }

    val reader =
      htsjdk.samtools.SamReaderFactory.makeDefault.open(file)

    var counterBases = 0L
    var counterBasesTarget = 0L
    reader.iterator.asScala
      .foreach { samRecord =>
        val mapq = samRecord.getMappingQuality
        val notPrimary = samRecord.isSecondaryAlignment
        val failsQC = samRecord.getReadFailsVendorQualityCheckFlag
        val properPair = samRecord.getProperPairFlag
        val readLength = samRecord.getReadLength
        val contigName = samRecord.getReferenceName

        val pass = mapq >= minimumMapQ && !notPrimary && !failsQC && properPair && readLength >= minimumReadLength

        if (pass) {
          var i = 0
          while (i < readLength) {
            val referenceOffset =
              samRecord.getReferencePositionAtReadPosition(i)
            if (inTarget(contigName, referenceOffset)) {
              counterBasesTarget += 1
            }
            counterBases += 1

            i += 1
          }
        }
      }

    reader.close

    (counterBases, counterBasesTarget)
  }

}
