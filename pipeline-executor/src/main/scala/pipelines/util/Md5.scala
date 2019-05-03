package org.gc.pipelines.util

import java.io._

object Md5 {

  /**
    * Returns the result of the block, and closes the resource.
    *
    * @param param closeable resource
    * @param f block using the resource
    */
  def useResource[A <: { def close(): Unit }, B](param: A)(f: A => B): B =
    try { f(param) } finally {
      import scala.language.reflectiveCalls
      param.close()
    }

  /** Reads file contents into a bytearray by fixed sized chunks. A block is executed on each chunk. */
  def readBinaryFileByChunks(fileName: File, chunkSize: Int)(
      func: Array[Byte] => Unit) {
    useResource(new BufferedInputStream(new FileInputStream(fileName))) { f =>
      val ar = new Array[Byte](chunkSize)
      var raw = f.read
      var i = 0
      while (raw != -1) {
        val ch: Byte = raw.toByte
        ar(i) = ch
        if (i == (chunkSize - 1)) {
          func(ar)
          i = 0
        } else i += 1
        raw = f.read
      }
      if (i != 0) {
        val ar2 = new Array[Byte](i)
        for (k <- 0 to i - 1) {
          ar2(k) = ar(k)
        }
        func(ar2)
      }

    }
  }

  def apply(fileName: File) = {
    val digest = java.security.MessageDigest.getInstance("MD5")
    val bufferSize = 8192
    readBinaryFileByChunks(fileName, bufferSize) { x =>
      digest.update(x)
    }
    javax.xml.bind.DatatypeConverter
      .printHexBinary(digest.digest())
      .toLowerCase()
  }

}
