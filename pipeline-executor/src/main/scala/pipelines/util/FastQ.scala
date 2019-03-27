package org.gc.pipelines.util

import java.io.{File, InputStream, FileInputStream}
import htsjdk.samtools.util._
import com.intel.gkl.compression.IntelDeflaterFactory
import com.intel.gkl.compression.IntelInflaterFactory

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

object FastQHelpers {

  def openOutputStream[T](fileName: File)(func: java.io.OutputStream => T): T =
    fileutils.useResource(
      new BlockCompressedOutputStream(fileName, 1, new IntelDeflaterFactory))(
      func)

  def openInputStream[T](fileName: File)(
      func: BlockCompressedInputStream => T): T =
    fileutils.useResource(
      new BlockCompressedInputStream(new FileInputStream(fileName),
                                     true,
                                     new IntelInflaterFactory))(func)

  case class VirtualPointerInterval(from: Long, byteLength: Long)
  object VirtualPointerInterval {
    implicit val encoder: Encoder[VirtualPointerInterval] =
      deriveEncoder[VirtualPointerInterval]
    implicit val decoder: Decoder[VirtualPointerInterval] =
      deriveDecoder[VirtualPointerInterval]
  }

  def indexFastQSplits(input: File,
                       maxReads: Long): Seq[VirtualPointerInterval] = {

    def loop(
        first: Byte,
        firstPointer: Long,
        inputStream: BlockCompressedInputStream,
        intervals: List[VirtualPointerInterval]): List[VirtualPointerInterval] =
      if (first == -1) intervals
      else {
        var b = first
        var pointer = firstPointer
        val maxLines = maxReads * 4
        val lineBreak = '\n'.toByte
        var i = 0L
        var byteCounter = 0L
        while (i < maxLines && b >= 0) {
          if (b == lineBreak) {
            i += 1L
            pointer = inputStream.getFilePointer
          }
          byteCounter += 1L
          b = inputStream.read.toByte
        }
        i / 4
        loop(b,
             pointer,
             inputStream,
             VirtualPointerInterval(firstPointer, byteCounter) :: intervals)

      }

    val isBgzip = Bgzf.isBlockCompressed(input)

    if (!isBgzip) Nil
    else
      openInputStream(input) { inputStream =>
        val pointer = inputStream.getFilePointer
        loop(inputStream.read.toByte, pointer, inputStream, Nil).reverse
      }

  }

  def splitFastQ(input: File, maxReads: Long): Seq[(File, Long)] = {

    def loop(first: Byte,
             inputStream: InputStream,
             files: List[(File, Long)]): List[(File, Long)] =
      if (first == -1) files
      else {
        val tmp = Files.createTempFile(".gz")
        var b = first
        val numberOfLines =
          openOutputStream(tmp) { os =>
            val maxLines = maxReads * 4
            val lineBreak = '\n'.toByte
            var i = 0L
            while (i < maxLines && b >= 0) {
              if (b == lineBreak) {
                i += 1L
              }
              os.write(b.toInt)
              b = inputStream.read.toByte
            }
            i / 4
          }
        loop(b, inputStream, (tmp, numberOfLines) :: files)

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
