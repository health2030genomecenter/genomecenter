package org.gc.fqsplit

import java.io.{File, InputStream, OutputStream}
import htsjdk.samtools.util._
import com.intel.gkl.compression.IntelDeflaterFactory
import com.intel.gkl.compression.IntelInflaterFactory

object FqSplitHelpers {

  def copy(is: InputStream, os: OutputStream, buffer: Array[Byte]) = {
    var bytesRead = is.read(buffer)
    while (bytesRead != -1) {
      os.write(buffer, 0, bytesRead)
      bytesRead = is.read(buffer)
    }
  }

  def zipOutputStream[T](os: java.io.OutputStream)(
      func: java.io.OutputStream => T): T =
    fileutils.useResource(
      new BlockCompressedOutputStream(os,
                                      null: File,
                                      0,
                                      new IntelDeflaterFactory))(func)

  def openInputStream[T](fileName: File)(
      func: BlockCompressedInputStream => T): T =
    fileutils.useResource(
      new BlockCompressedInputStream(fileName, new IntelInflaterFactory))(func)

  def readFastQSplit[T](input: File, from: Long, byteLength: Long)(
      func: InputStream => T): T = {
    openInputStream(input) { inputStream =>
      inputStream.seek(from)
      val limited = new InputStream {
        var b = 0L
        val max = byteLength
        def read =
          if (b < max) {
            b += 1
            inputStream.read
          } else -1
      }
      func(limited)
    }
  }

}
