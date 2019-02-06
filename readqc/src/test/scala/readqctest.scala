package org.gc.readqc

import org.scalatest._
import java.io._

class ReadQCTest extends FunSuite with Matchers {

  test("read qc ") {
    val fastq =
      new File(getClass.getResource("/tutorial_8017/papa.read1.fq.gz").getFile)
    val qc = ReadQC.process(List(fastq))
    qc.cycles.size shouldBe 151

  }

}
