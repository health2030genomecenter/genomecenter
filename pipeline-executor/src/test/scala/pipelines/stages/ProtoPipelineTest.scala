package org.gc.pipelines.stages

import org.scalatest._

import java.io.File

import org.gc.pipelines.model._
import org.gc.pipelines.util.StableSet
import org.gc.pipelines.application.{
  RunfolderReadyForProcessing,
  DemultiplexingConfiguration,
  RunConfiguration,
  WESConfiguration,
  InputFastQPerLane,
  InputSampleAsFastQ
}
import org.gc.pipelines.util.StableSet
import org.gc.pipelines.util.StableSet._
import tasks.fileservice._

class ProtopipelineTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("parse RunInfo.xml") {
    new Fixture {
      ProtoPipelineStages.parseReadLengthFromRunInfoXML(runInfoContent) shouldBe Map(
        1 -> 50,
        2 -> 7,
        3 -> 50)
    }
  }

  test("SingleSampleResult should deserialize from old data") {
    new Fixture {
      val sf = SharedFile(ManagedFilePath(Vector()), 123, 13)

      val wesResult: SingleSamplePipelineResult = SingleSamplePipelineResult(
        alignedLanes = StableSet(
          BamWithSampleMetadataPerLane(project = Project("project1"),
                                       sampleId = SampleId("smapleId1"),
                                       runId = runId,
                                       lane = Lane(1),
                                       bam = Bam(sf))
        ),
        mergedRuns = Some(
          PerSampleMergedWESResult(
            bam = CoordinateSortedBam(sf, sf),
            haplotypeCallerReferenceCalls = Some(VCF(sf, None)),
            gvcf = Some(VCF(sf, Some(sf))),
            project = Project("project1"),
            sampleId = SampleId("sampleId1"),
            alignmentQC = AlignmentQCResult(sf, sf, sf, sf, sf, sf, sf),
            duplicationQC = DuplicationQCResult(sf),
            targetSelectionQC = SelectionQCResult(sf),
            wgsQC = CollectWholeGenomeMetricsResult(sf),
            gvcfQCInterval = Some(VariantCallingMetricsResult(sf, sf)),
            gvcfQCOverall = Some(VariantCallingMetricsResult(sf, sf)),
            referenceFasta = IndexedReferenceFasta(sf, List(sf).toSet.toStable),
            coverage = MeanCoverageResult(1d, 1d)
          ))
      )

      val wesConfig: SingleSampleConfiguration = SingleSampleConfiguration(
        analysisId = AnalysisId("aid"),
        dbSnpVcf = VCF(sf, Some(sf)),
        vqsrTrainingFiles = Some(
          VQSRTrainingFiles(millsAnd1Kg = VCF(sf, None),
                            oneKgHighConfidenceSnps = VCF(sf, None),
                            hapmap = VCF(sf, None),
                            oneKgOmni = VCF(sf, None),
                            dbSnp138 = VCF(sf, None))),
        wesConfiguration = wesConfiguration,
        variantCallingContigs = Some(ContigsFile(sf))
      )

      val rna: SingleSamplePipelineResultRNA = SingleSamplePipelineResultRNA(
        analysisId = AnalysisId("aid"),
        star = StarResult(
          finalLog = sf,
          runId = RunId("runid"),
          bam = BamWithSampleMetadata(project = Project("p1"),
                                      sampleId = SampleId("s1"),
                                      bam = Bam(sf))
        ),
        quantification = QTLToolsQuantificationResult(sf, sf, sf, sf, sf)
      )
      val demux1: PerSamplePerRunFastQ = PerSamplePerRunFastQ(
        lanes = List(
          FastQPerLane(runId = RunId("r1"),
                       lane = Lane(1),
                       read1 = FastQ(sf, 1L, None),
                       read2 = FastQ(sf, 1L, None),
                       umi = Some(FastQ(sf, 1L, None)),
                       partition = PartitionId(1))).toSet.toStable,
        project = Project("p1"),
        sampleId = SampleId("s1"),
        runId = RunId("r1")
      )
      val fastp: FastpReport =
        FastpReport(sf, sf, Project("p1"), SampleId("s1"), RunId("r1"))
      val runFolder: RunfolderReadyForProcessing = RunfolderReadyForProcessing(
        runId = RunId("r1"),
        runFolderPath = Some("path1"),
        demultiplexedSamples = Some(
          List(
            InputSampleAsFastQ(
              Set(
                InputFastQPerLane(
                  Lane(1),
                  "path1",
                  "path2",
                  Some("path3"),
                  None,
                  None,
                  None
                )
              ),
              Project("p1"),
              SampleId("s1")
            ))),
        runConfiguration = runConfiguration
      )

      val old = SampleResult(
        wes = List((wesResult, wesConfig)),
        rna = List(rna),
        demultiplexed = List(demux1),
        fastpReports = List(fastp),
        runFolders = List(runFolder),
        project = Project("project1"),
        sampleId = SampleId("sampleId1")
      )
      import io.circe.syntax._
      import io.circe.parser._
      val serialized: String = old.asJson.toString
      val _ = serialized
      // println(serialized)
      val oldDeserialized = decode[SampleResult](
        scala.io.Source
          .fromInputStream(getClass.getResourceAsStream("/SampleResult.js"))
          .mkString).right.get

      oldDeserialized.toString shouldBe old.toString
    }
  }

  trait Fixture {

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

    val sampleSheetFile = new File("somePath")

    val globalIndexSetFilePath = getClass
      .getResource("/indices")
      .getFile

    val referenceFasta = getClass
      .getResource("/tutorial_8017/chr19_chr19_KI270866v1_alt.fasta")
      .getFile

    val targetIntervals = getClass
      .getResource("/tutorial_8017/capture.bed")
      .getFile

    val knownSitesVCF = new File(
      getClass
        .getResource("/example.vcf.gz")
        .getFile)

    val runId = RunId("whateverRunId")

    val wesConfiguration = WESConfiguration(
      analysisId = AnalysisId("default"),
      referenceFasta = "file",
      targetIntervals = "file",
      bqsrKnownSites = StableSet("file"),
      dbSnpVcf = "file",
      variantEvaluationIntervals = "file",
      vqsrMillsAnd1Kg = None,
      vqsrHapmap = None,
      vqsrOneKgOmni = None,
      vqsrOneKgHighConfidenceSnps = None,
      vqsrDbSnp138 = None,
      doVariantCalls = Some(true),
      doJointCalls = Some(true),
      minimumTargetCoverage = None,
      minimumWGSCoverage = None,
      variantCallingContigs = None
    )

    val runConfiguration = RunConfiguration(
      demultiplexingRuns = StableSet(
        DemultiplexingConfiguration(
          demultiplexingId = DemultiplexingId("demultiplexOnce"),
          sampleSheet = "file",
          extraBcl2FastqArguments = Seq("--tiles",
                                        "s_1_1101",
                                        "--use-bases-mask",
                                        "y75n,i6n*,n10,y75n"),
          readAssignment = (1, 2),
          umi = None,
          tenX = None,
          partitionByLane = None,
          partitionByTileCount = None
        ),
        DemultiplexingConfiguration(
          demultiplexingId = DemultiplexingId("demultiplexTwice"),
          sampleSheet = "file",
          extraBcl2FastqArguments = Seq("--tiles",
                                        "s_1_1101",
                                        "--use-bases-mask",
                                        "y75n,i6n*,n10,y75n"),
          readAssignment = (1, 2),
          umi = None,
          tenX = None,
          partitionByLane = None,
          partitionByTileCount = None
        )
      ),
      globalIndexSet = Some("file"),
      StableSet.empty
    )

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
}
