package org.gc.pipelines.util

import java.io.{File, InputStream, FileInputStream}
import htsjdk.samtools.util._
import com.intel.gkl.compression.IntelDeflaterFactory
import com.intel.gkl.compression.IntelInflaterFactory

object FastQHelpers {
  def splitFastQ(input: File, maxReads: Long): Seq[File] = {

    def openOutputStream[T](fileName: File)(
        func: java.io.OutputStream => T): T =
      fileutils.useResource(
        new BlockCompressedOutputStream(fileName, 0, new IntelDeflaterFactory))(
        func)

    def openInputStream[T](fileName: File)(func: java.io.InputStream => T): T =
      fileutils.useResource(
        new BlockCompressedInputStream(new FileInputStream(fileName),
                                       true,
                                       new IntelInflaterFactory))(func)

    def loop(first: Byte,
             inputStream: InputStream,
             files: List[File]): List[File] =
      if (first == -1) files
      else {
        val tmp = Files.createTempFile(".gz")
        var b = first
        openOutputStream(tmp) { os =>
          var i = 0L
          val maxLines = maxReads * 4
          val lineBreak = '\n'.toByte
          while (i < maxLines && b >= 0) {
            if (b == lineBreak) {
              i += 1L
            }
            os.write(b.toInt)
            b = inputStream.read.toByte
          }
        }
        loop(b, inputStream, tmp :: files)

      }

    openInputStream(input) { inputStream =>
      loop(inputStream.read.toByte, inputStream, Nil).reverse
    }

  }
  def getNumberOfReads(file: File): Long = {

    fileutils.openSource(file) { source =>
      val it = source.getLines
      var c = 0L
      while (it.hasNext) {
        it.next
        c += 1
      }
      c / 4
    }
  }
}
