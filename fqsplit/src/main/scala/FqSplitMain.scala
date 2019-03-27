package org.gc.fqsplit

object fqsplit {
  def main(args: Array[String]): Unit = {
    new FqSplitCLI(args)
  }
}

class FqSplitCLI(args: Array[String]) {
  def printHelp() = {
    println(
      """Usage:
      java -jar jar file interval_from interval_length
        Reads bgzipped fastq and emits level0 compressed fastq of the specified interval
      java -jar jar --help 
        Prints this message""")
  }
  if (args.contains("--help")) {
    printHelp()
  } else {
    val file = new java.io.File(args(0))
    val from = args(1).toLong
    val length = args(2).toLong
    val os = System.out
    val buffer = Array.ofDim[Byte](8192)
    FqSplitHelpers.zipOutputStream(os) { os =>
      FqSplitHelpers.readFastQSplit(file, from, length) { is =>
        FqSplitHelpers.copy(is, os, buffer)
      }
    }
  }
}
