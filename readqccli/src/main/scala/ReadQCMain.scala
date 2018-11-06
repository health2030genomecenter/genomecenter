package org.gc.readqc

import scala.collection.JavaConverters._

object main {
  def main(args: Array[String]): Unit = {
    new ReadQCCli(args)
  }
}

class ReadQCCli(args: Array[String]) {
  def printHelp() = {
    println(
      """Usage:
      java -jar jar 
        Reads unzipped fastq data from stdin and writes a json report to the stdout.
      java -jar jar --help 
        Prints this message""")
  }
  if (args.contains("--help")) {
    printHelp()
  } else {
    val reader =
      new java.io.BufferedReader(new java.io.InputStreamReader(System.in))
    val fqIterator =
      new htsjdk.samtools.fastq.FastqReader(reader).iterator
    val metrics = ReadQC.processHtsJdkRecords(fqIterator.asScala)
    import io.circe.syntax._
    println(metrics.asJson.noSpaces)
  }
}
