package org.gc.pipelines.util

import java.io.File
import java.nio.file.{Files => JFiles, FileSystems}
import scala.collection.JavaConverters._

object Files {

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
