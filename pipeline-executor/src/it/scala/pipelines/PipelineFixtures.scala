package org.gc.pipelines

import org.gc.pipelines.util.Exec
import java.io.File
import org.gc.pipelines.model.{SampleSheet, RunId}

object PipelineFixtures {

  def extractRunFolderTestData = {
    val tmpFolder = fileutils.TempFile.createTempFolder(".runfolder_test")
    val testDataTarFile =
      getClass
        .getResource("/180622_M04914_0002_000000000-BWHDL.tar.gz")
        .getFile
    Exec.bash("test.demultiplex")(
      s"cd ${tmpFolder.getAbsolutePath} && tar xf $testDataTarFile ")
    new File(tmpFolder, "180622_M04914_0002_000000000-BWHDL").getAbsolutePath
  }

  val read1 = getClass
    .getResource("/tutorial_8017/papa.read1.fq.gz")
    .getFile

  val read2 = getClass
    .getResource("/tutorial_8017/papa.read2.fq.gz")
    .getFile

  val referenceFasta = getClass
    .getResource("/tutorial_8017/chr19_chr19_KI270866v1_alt.fasta")
    .getFile

  val targetIntervals = getClass
    .getResource("/tutorial_8017/capture.bed")
    .getFile

  val globalIndexSetFilePath = getClass
    .getResource("/indices")
    .getFile

  val knownSitesVCF = new File(
    getClass
      .getResource("/example.vcf.gz")
      .getFile)

  val runId = RunId("whateverRunId")

  val sampleSheet = SampleSheet(
    s"""[Header],,,,,,,,,,
IEMFileVersion,5,,,,,,,,,
Investigator Name,GC,,,,,,,,,
Experiment Name,Training_Miseq_22062018,,,,,,,,,
Date,22/06/2018,,,,,,,,,
Workflow,GenerateFASTQ,,,,,,,,,
Application,FASTQ Only,,,,,,,,,
Instrument Type,MiSeq,,,,,,,,,
Assay,TruSeq DNA PCR-Free,,,,,,,,,
Index Adapters,IDT-ILMN TruSeq DNA UD Indexes (24 Indexes),,,,,,,,,
Description,,,,,,,,,,
Chemistry,Amplicon,,,,,,,,,
,,,,,,,,,,
[Reads],,,,,,,,,,
76,,,,,,,,,,
76,,,,,,,,,,
,,,,,,,,,,
[Settings],,,,,,,,,,
ReverseComplement,0,,,,,,,,,
,,,,,,,,,,
[Data],,,,,,,,,,
Sample_ID,Sample_Name,Sample_Plate,Sample_Well,Index_Plate_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description,Lane
GIB,GIB,,,F01,AD007,CAGATC,MolBC,NNNNNNNNNN,project1,,001
sample2,sample2,,,boo,boo,ATCACG,MolBC,NNNNNNNNNN,project2,,001
  """
  )

  val sampleSheet2 = SampleSheet(
    s"""[Header],,,,,,,,,,
IEMFileVersion,5,,,,,,,,,
Investigator Name,GC,,,,,,,,,
Experiment Name,Training_Miseq_22062018,,,,,,,,,
Date,22/06/2018,,,,,,,,,
Workflow,GenerateFASTQ,,,,,,,,,
Application,FASTQ Only,,,,,,,,,
Instrument Type,MiSeq,,,,,,,,,
Assay,TruSeq DNA PCR-Free,,,,,,,,,
Index Adapters,IDT-ILMN TruSeq DNA UD Indexes (24 Indexes),,,,,,,,,
Description,,,,,,,,,,
Chemistry,Amplicon,,,,,,,,,
,,,,,,,,,,
[Reads],,,,,,,,,,
76,,,,,,,,,,
76,,,,,,,,,,
,,,,,,,,,,
[Settings],,,,,,,,,,
ReverseComplement,0,,,,,,,,,
,,,,,,,,,,
[Data],,,,,,,,,,
Sample_ID,Sample_Name,Sample_Plate,Sample_Well,Index_Plate_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description,Lane
GIB2,GIB2,,,F01,AD007,CAGATC,MolBC,NNNNNNNNNN,project1,,001
sample3,sample3,,,boo,boo,ATCACG,MolBC,NNNNNNNNNN,project3,,001
  """
  )
  val sampleSheetFile =
    fileutils.openFileWriter(_.write(sampleSheet.sampleSheetContent))._1

  val sampleSheetFile2 =
    fileutils.openFileWriter(_.write(sampleSheet2.sampleSheetContent))._1
  val runFolderPath = extractRunFolderTestData

  val gtfFile = new File(
    getClass
      .getResource("/short.gtf")
      .getFile)

  val runInfoContent =
    """<?xml version="1.0"?>
<RunInfo xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="2">
<Run Id="141008_7001253F_0202_AC5E7AANXX" Number="202">
  <Flowcell>C5E7AANXX</Flowcell>
  <Instrument>7001253F</Instrument>
  <Date>141008</Date>
  <Reads>
    <Read Number="1" NumCycles="50" IsIndexedRead="N" />
    <Read Number="2" NumCycles="7" IsIndexedRead="Y" />
    <Read Number="3" NumCycles="50" IsIndexedRead="N" />
  </Reads>
  <FlowcellLayout LaneCount="8" SurfaceCount="2" SwathCount="3" TileCount="16" />
  <AlignToPhiX>
    <Lane>3</Lane>
    <Lane>4</Lane>
    <Lane>5</Lane>
    <Lane>6</Lane>
    <Lane>7</Lane>
    <Lane>8</Lane>
  </AlignToPhiX>
</Run>
</RunInfo>
"""

}
