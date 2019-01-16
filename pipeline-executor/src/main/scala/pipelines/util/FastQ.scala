package org.gc.pipelines.util

import java.io.File

object FastQHelpers {
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
