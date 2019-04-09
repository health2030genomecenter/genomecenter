package org.gc.pipelines.util

import java.io.File

object Bed {

  def size(f: File): Long = {
    fileutils
      .openSource(f)(
        _.getLines
          .filterNot(_.startsWith("#"))
          .map { line =>
            val spl = line.split("\t")
            intervaltree
              .IntervalWithPayLoad(spl(1).toInt, spl(2).toInt, ())
              .size
              .toLong
          }
          .sum
      )
  }

}
