package org.gc.umiprocessor

import org.scalatest._
import htsjdk.samtools.fastq._
import htsjdk.samtools._
import java.io._
import scala.collection.JavaConverters._

class CopyUmiToOXTest extends FunSuite with Matchers {

  test("copy correctly ") {
    val (fastq, bam) = setup
    val is = new FileInputStream(bam)
    val generatedBam = fileutils.openFileOutputStream { os =>
      val count = CopyUmiToOX.copy(is, fastq, os)
      count shouldBe 2
    }._1
    is.close
    val samReader =
      SamReaderFactory.makeDefault.open(SamInputResource.of(generatedBam))
    val reads = samReader.iterator.asScala.toList
    samReader.close
    reads.head.getReadName shouldBe "readname1"
    reads.head.getReadString shouldBe "AAAA"
    reads.head.getAttribute("OX").toString shouldBe "CCCC"

    reads(1).getReadName shouldBe "readname2"
    reads(1).getReadString shouldBe "TTTT"
    reads(1).getAttribute("OX").toString shouldBe "GG"

  }

  def setup = {
    val reads = List(("readname1", "AAAA", "CCCC"), ("readname2", "TTTT", "GG"))
    def writeFastq = {
      val file = fileutils.TempFile.createTempFile(".fastq")
      val writer = (new FastqWriterFactory).newWriter(file)
      reads.foreach {
        case (name, _, umi) =>
          writer.write(new FastqRecord(name, umi, "+", umi))
      }
      writer.close
      file
    }
    def writeBam = {
      val file = fileutils.TempFile.createTempFile(".bam")
      val header = new SAMFileHeader
      val writer = (new SAMFileWriterFactory).makeBAMWriter(header, true, file)
      reads.foreach {
        case (name, read, _) =>
          val r = new SAMRecord(header)
          r.setReadName(name)
          r.setReadUnmappedFlag(true)
          r.setReadString(read)
          writer.addAlignment(r)
      }
      writer.close
      file
    }
    (writeFastq, writeBam)
  }
}
