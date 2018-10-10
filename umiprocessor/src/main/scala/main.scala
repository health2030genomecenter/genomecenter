package org.gc.umiprocessor

import java.io._

object main {
  def main(args: Array[String]): Unit = {
    new UmiProcessor(args)
  }
}

class UmiProcessor(args: Array[String]) {
  def printHelp() = {
    println(
      """Usage:
      java -jar jar umi.fastq
        Joins a bam stream on standard input with the contents of the umi.fastq file. The two files must have the same ordering. Copies the sequence content of the fastq file to the OX tag of the sam records. Writes an uncompressed bam stream to the standard output. Diagnostic logs are written to standard error.    
      java -jar jar --help 
        Prints this message""")
  }
  if (args.size < 1) {
    printHelp()
    System.exit(0)
  } else {
    val program = args(0)
    program.toLowerCase match {
      case "--help" | "help" | "-h" | "h" =>
        printHelp()
      case _ =>
        System.err.println(s"Got cli arguments: ${args.toList}")
        val fqFile = new File(args(0))
        val count = CopyUmiToOX.copy(System.in, fqFile, System.out)
        System.err.println(s"Processed $count records.")

    }
  }
}
