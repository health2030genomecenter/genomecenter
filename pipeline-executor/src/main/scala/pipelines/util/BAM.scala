package org.gc.pipelines.util

object BAM {

  def getSortOrder(file: java.io.File) = {
    import htsjdk.samtools.SamReaderFactory
    val reader = SamReaderFactory.makeDefault.open(file)
    val sortOrder = reader.getFileHeader.getSortOrder
    reader.close
    sortOrder
  }

}
