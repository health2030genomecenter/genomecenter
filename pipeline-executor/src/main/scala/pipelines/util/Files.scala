package org.gc.pipelines.util

import java.io.File
import java.nio.file.{Files => JFiles}
import scala.collection.JavaConverters._

object Files {
  def list(folder: File, glob: String): List[File] = {
    val stream = JFiles.newDirectoryStream(folder.toPath, glob)
    try {
      stream.asScala.map(_.toFile).toList
    } finally {
      stream.close()
    }
  }
}
