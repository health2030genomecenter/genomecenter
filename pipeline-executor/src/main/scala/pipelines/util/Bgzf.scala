package org.gc.pipelines.util

import java.io.{
  InputStream,
  OutputStream,
  BufferedOutputStream,
  File,
  FileOutputStream
}
import htsjdk.samtools.util.BlockCompressedInputStream
import htsjdk.samtools.util.BlockCompressedStreamConstants

object Bgzf {

  def isBlockCompressed(f: File): Boolean = fileutils.openFileInputStream(f) {
    is =>
      BlockCompressedInputStream.isValidFile(is)
  }

  def partition(f: File, maxFileSize: Long): List[File] =
    fileutils.openFileInputStream(f)(partition(_, maxFileSize))

  /** redistribute gzip blocks
    */
  def partition(inputStream: InputStream, maxFileSize: Long): List[File] = {
    val bgzfInputStream = new MyBgzfInputStream(inputStream)
    val blocks = Iterator
      .continually(bgzfInputStream.readNextBlock)
      .takeWhile(_.nonEmpty)
    val files = scala.collection.mutable.ArrayBuffer[(File, OutputStream)]()
    var currentSize = 0L
    def openNewPartition() = {
      currentSize = 0L
      val idx = files.size
      val file = Files.createTempFile(".part" + idx)
      files.lastOption.foreach(_._2.close)
      val os = new BufferedOutputStream(new FileOutputStream(file))
      files.append(file -> os)
    }

    def writeBlock(block: Array[Byte]) = {
      currentSize += block.size
      files.last._2.write(block)
    }

    openNewPartition()

    blocks.foreach { block =>
      if (currentSize + block.length < maxFileSize) {
        writeBlock(block)
      } else {
        openNewPartition()
        writeBlock(block)
      }

    }

    files.lastOption.foreach(_._2.close)
    files.map(_._1).toList

  }

  private class MyBgzfInputStream(inputStream: InputStream)
      extends BlockCompressedInputStream(inputStream) {
    private val mFileBuffer: Array[Byte] =
      Array.ofDim(BlockCompressedStreamConstants.MAX_COMPRESSED_BLOCK_SIZE);

    private def unpackInt16(buffer: Array[Byte], offset: Int): Int =
      ((buffer(offset) & 0xFF) |
        ((buffer(offset + 1) & 0xFF) << 8))

    private def readBytes(offset: Int, length: Int): Int = {
      var bytesRead = 0;
      var done = false
      while (bytesRead < length && !done) {
        val count =
          inputStream.read(mFileBuffer, offset + bytesRead, length - bytesRead);
        if (count <= 0) {
          done = true
        } else {
          bytesRead += count;
        }
      }
      bytesRead
    }

    def readNextBlock = {
      val headerByteCount =
        readBytes(0, BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH);
      if (headerByteCount == 0) {
        // Handle case where there is no empty gzip block at end.
        Array.empty[Byte]
      } else {
        if (headerByteCount != BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH) {
          throw new RuntimeException("Invalid block header length")
        }
        val blockLength = unpackInt16(
          mFileBuffer,
          BlockCompressedStreamConstants.BLOCK_LENGTH_OFFSET) + 1;
        if (blockLength < BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH || blockLength > mFileBuffer.length) {
          throw new RuntimeException("Invalid block header length")
        }
        val remaining = blockLength - BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH;
        val dataByteCount = readBytes(
          BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH,
          remaining);
        if (dataByteCount != remaining) {
          throw new RuntimeException("Truncated file")
        }
        mFileBuffer.take(blockLength)

      }
    }
  }

}
