package org.gc.pipelines.util

import scala.collection.JavaConverters._
import java.io.File
import com.typesafe.scalalogging.StrictLogging

object MappedBases extends StrictLogging {

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
    println(intervaltrees)

    logger.debug(
      s"Interval trees with contigs: ${intervaltrees.map(_.keySet.toSeq.sorted)}")

    def inTarget(contig: String, bp: Int) = intervaltrees match {
      case None => true
      case Some(trees) =>
        trees
          .get(contig)
          .map(
            tree =>
              intervaltree.IntervalTree
                .lookup(intervaltree.IntervalWithPayLoad(bp, bp + 1, ()), tree)
                .nonEmpty)
          .getOrElse(false)
    }

    val reader =
      htsjdk.samtools.SamReaderFactory.makeDefault.open(file)

    var counterBases = 0L
    var counterBasesTarget = 0L
    var countNonPass = 0L
    var countOutOfTarget = 0L
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
            // I assume htsjdk returns 1 based
            val referenceOffset =
              samRecord.getReferencePositionAtReadPosition(i) - 1
            if (inTarget(contigName, referenceOffset)) {
              counterBasesTarget += 1
            } else {
              countOutOfTarget += 1L
            }
            counterBases += 1

            i += 1
          }
        } else {
          countNonPass += 1L
        }
      }

    reader.close

    logger.debug(
      s"Count bases in $file. non pass: $countNonPass, out of target: $countOutOfTarget")

    (counterBases, counterBasesTarget)
  }

}
