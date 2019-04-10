package org.gc.pipelines.util

import java.io.File
import java.nio.file.{Files => JFiles, FileSystems}
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.StrictLogging

object Files extends StrictLogging {

  private val tempFiles = scala.collection.concurrent.TrieMap[File, File]()

  scala.sys.addShutdownHook {
    val size = tempFiles.size
    logger.info(s"Deleting $size temp files.")
    tempFiles.foreach { p =>
      logger.info("Deleting " + p._2.toString)
      p._2.delete
    }
    logger.info(s"Deleted $size temp files.")
  }

  def createTempFile(suffix: String) = {
    val f = fileutils.TempFile.createTempFile(suffix)
    f.deleteOnExit
    tempFiles.update(f, f)
    f
  }

  def deleteRecursively(folder: File): Unit =
    JFiles
      .walk(folder.toPath)
      .iterator
      .asScala
      .filter(f => JFiles.isRegularFile(f))
      .map(_.toFile)
      .foreach(_.delete)

  def list(folder: File, glob: String): List[File] = {
    val pathMatcher = FileSystems.getDefault.getPathMatcher("glob:" + glob)
    val stream =
      JFiles.walk(folder.toPath)
    try {
      stream.iterator.asScala
        .filter(path => pathMatcher.matches(path))
        .map(_.toFile)
        .toList
    } finally {
      stream.close()
    }
  }
}
