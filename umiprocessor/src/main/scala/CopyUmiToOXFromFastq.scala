package org.gc.umiprocessor

import htsjdk.samtools.{
  SamReaderFactory,
  SamInputResource,
  SAMFileWriterFactory
}
import htsjdk.samtools.fastq.FastqReader

import htsjdk.samtools.util.SequenceUtil
import java.io._

object CopyUmiToOX {

  def copy(bam: InputStream, umiFastq: File, output: OutputStream) = {
    val samReader =
      SamReaderFactory.makeDefault.open(SamInputResource.of(bam))
    val header = samReader.getFileHeader
    val samWriter = {
      val fac = new SAMFileWriterFactory
      fac.setCompressionLevel(0)
      fac
    }.makeBAMWriter(header, true, output)
    val umiReader = new FastqReader(umiFastq)
    val samIterator = samReader.iterator
    var counter = 0

    while (umiReader.hasNext) {
      if (!samIterator.hasNext)
        throw new RuntimeException(
          "sam and umi fq files should have the same length")
      else {
        counter += 1
        val umiFqRecord = umiReader.next
        val umiSequence = umiFqRecord.getReadString
        val umiReadName =
          SequenceUtil.getSamReadNameFromFastqHeader(umiFqRecord.getReadName)
        val readRecord = samIterator.next
        assert(
          readRecord.getReadName == umiReadName,
          s"Umi read name != sam read name. The two files must have the same ordering. $umiFqRecord vs $readRecord")
        readRecord.setAttribute("OX", umiSequence)
        samWriter.addAlignment(readRecord)
      }
    }
    samWriter.close

    counter
  }

}
