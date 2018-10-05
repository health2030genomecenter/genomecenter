package org.gc.pipelines.model

import org.scalatest._

class SampleSheetTest extends FunSuite with Matchers {

  test(
    "Sample sheet should be parsed correctly, including the [GenomeCenter] section") {
    new Fixture {
      val parsed = SampleSheet(exampleSampleSheetContent).parsed
      parsed.runId shouldBe None
      parsed.sampleIds shouldBe Seq("A10001", "A10002", "A10003", "A10004")
      parsed.lanes shouldBe Seq("L1", "L2")
      parsed.projects shouldBe Seq("proj")
      parsed.poolingLayout.size shouldBe 4
      parsed.poolingLayout.take(1) shouldBe List(
        SampleSheet.Multiplex(SampleId("A10001"),
                              Project("proj"),
                              Lane("L1"),
                              Index("ATTACTCG"),
                              Some(Index("TATAGCCT")))
      )

    }

  }

  trait Fixture {
    val exampleSampleSheetContent =
      """[Header]
Date,2017-04-05
Workflow,GenerateFASTQ
Application,FASTQ Only
Assay,TruSeq HT
Description,
Chemistry,Amplicon
[Reads]
151
151
[Settings]
Adapter,AGATCGGAAGAGCACACGTCTGAACTCCAGTCA
AdapterRead2,AGATCGGAAGAGCGTCGTGTAGGGAAAGAGT
[GenomeCenter]
customKey,customValue
bcl2fastqArguments,["--flag","stuff,with,comma"]
[Data]
Sample_ID,Sample_Name,I7_Index_ID,index,I5_Index_ID,index2,Lane,Sample_Project
A10001,Sample_A,D701,ATTACTCG,D501,TATAGCCT,L1,proj
A10002,Sample_B,D702,TCCGGAGA,D501,TATAGCCT,L1,proj
A10003,Sample_C,D703,CGCTCATT,D501,TATAGCCT,L2,proj
A10004,Sample_D,D704,GAGATTCC,D501,TATAGCCT,L2,proj
"""
  }

}
