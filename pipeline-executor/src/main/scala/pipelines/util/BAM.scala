package org.gc.pipelines.util

import htsjdk.samtools.SamFileValidator
import scala.collection.JavaConverters._
import java.io.File

object BAM {

  def getSortOrder(file: java.io.File) = {
    import htsjdk.samtools.SamReaderFactory
    val reader = SamReaderFactory.makeDefault.open(file)
    val sortOrder = reader.getFileHeader.getSortOrder
    reader.close
    sortOrder
  }

  def getMeanReadLength(file: java.io.File, take: Int) = {
    import htsjdk.samtools.SamReaderFactory
    val reader = SamReaderFactory.makeDefault.open(file)
    val readLengths =
      reader.iterator.asScala.take(take).map(_.getReadLength).toList

    reader.close
    readLengths.sum.toDouble / readLengths.size
  }

  def validate(file: java.io.File, reference: File) = {

    import htsjdk.samtools.SamReaderFactory
    import htsjdk.samtools.reference.FastaSequenceFile
    val reader = SamReaderFactory.makeDefault.open(file)

    val refFile = new FastaSequenceFile(reference, false)
    val validator =
      new SamFileValidator(new java.io.PrintWriter(System.out), 100)
    validator.validateSamFileSummary(reader, refFile)
  }

}
