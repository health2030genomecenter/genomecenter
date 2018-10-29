package org.gc.pipelines.util

import scala.collection.JavaConverters._

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

}
