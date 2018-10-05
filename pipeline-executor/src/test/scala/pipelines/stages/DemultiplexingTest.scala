package org.gc.pipelines.stages

import org.scalatest._

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

import tasks._
import com.typesafe.config.ConfigFactory
import fileutils.TempFile
import java.io.File

import org.gc.pipelines.util.Exec
import org.gc.pipelines.model._

class DemultiplexingTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("demultiplexing stats should deserialize") {

    val rawStats = io.circe.parser
      .decode[DemultiplexingStats.Root](statsFileContent)
      .right
      .get
    val summaryStat = DemultiplexingSummary.fromStats(rawStats, Map())
    println(DemultiplexingSummary.renderAsTable(summaryStat))
  }

  test("Demultiplexing stage should demultiplex a simple run") {
    new Fixture {

      Given("a runfolder")
      When("executing the Demultiplexing.allLanes task")
      val result = withTaskSystem(testConfig) { implicit ts =>
        val future = Demultiplexing.allLanes(
          DemultiplexingInput(
            runId = RunId(runId),
            runFolderPath = runFolderPath,
            sampleSheet = SampleSheetFile(
              await(SharedFile(sampleSheetFile, "sampleSheet"))),
            extraBcl2FastqCliArguments = Seq("--tiles",
                                             "s_1_1101",
                                             "--use-bases-mask",
                                             "y75n,i6n*,n10,y75n")
          ))(CPUMemoryRequest(1, 500))
        import scala.concurrent.duration._
        scala.concurrent.Await.result(future, atMost = 400000 seconds)

      }

      Then(
        "a run and lane specific folder should be created at the root of the storage")
      val outputFolder =
        new File(basePath.getAbsolutePath + s"/demultiplex/$runId/L001")
      outputFolder.canRead shouldBe true

      And("uncaptured output files from bcl2fastq should be present")
      val statsFolder = new File(outputFolder.getAbsolutePath + "/Stats")
      statsFolder.canRead shouldBe true
      val demultiplexStatFile =
        new File(statsFolder.getAbsolutePath + "/DemuxSummaryF1L1.txt")
      demultiplexStatFile.canRead shouldBe true

      And(
        "captured fastq files should be present for the demultiplexed samples and for the undetermined reads")
      result.get.fastqs.size shouldBe 6
      result.get.fastqs.toList.map(_.sampleId).toSet shouldBe Set(
        "Undetermined",
        "GIB",
        "sample2")
      result.get.fastqs.toList.map(_.readType).toSet shouldBe Set("R2", "R1")
      result.get.fastqs.toList.map(_.lane).toSet shouldBe Set("L001")

    }
  }

  trait Fixture {
    def extractRunFolderTestData = {
      val tmpFolder = TempFile.createTempFolder(".runfolder_test")
      val testDataTarFile =
        getClass
          .getResource("/180622_M04914_0002_000000000-BWHDL.tar.gz")
          .getFile
      Exec.bash("test.demultiplex")(
        s"cd ${tmpFolder.getAbsolutePath} && tar xf $testDataTarFile ")
      new File(tmpFolder, "180622_M04914_0002_000000000-BWHDL").getAbsolutePath
    }

    val runId = "whateverRunId"
    val sampleSheet = SampleSheet(
      """[Header],,,,,,,,,,
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
GIB,GIB,,,F01,AD007,CAGATC,MolBC,NNNNNNNNNN,,,001
      """
    )
    val sampleSheetFile =
      fileutils.openFileWriter(_.write(sampleSheet.sampleSheetContent))._1
    val runFolderPath = extractRunFolderTestData

    val (testConfig, basePath) = makeTestConfig
  }
}

trait TestHelpers {

  def fetchIndexedReference(fasta: File)(implicit tsc: TaskSystemComponents) = {
    val liftedFasta = await(SharedFile(fasta, "referenceFasta.fasta"))
    val indexFiles = List(
      await(
        SharedFile(
          new File(fasta.getAbsolutePath.stripSuffix("fasta") + "dict"),
          "referenceFasta.dict")),
      await(
        SharedFile(new File(fasta.getAbsolutePath + ".amb"),
                   "referenceFasta.fasta.amb")),
      await(
        SharedFile(new File(fasta.getAbsolutePath + ".ann"),
                   "referenceFasta.fasta.ann")),
      await(
        SharedFile(new File(fasta.getAbsolutePath + ".bwt"),
                   "referenceFasta.fasta.bwt")),
      await(
        SharedFile(new File(fasta.getAbsolutePath + ".fai"),
                   "referenceFasta.fasta.fai")),
      await(
        SharedFile(new File(fasta.getAbsolutePath + ".pac"),
                   "referenceFasta.fasta.pac")),
      await(
        SharedFile(new File(fasta.getAbsolutePath + ".sa"),
                   "referenceFasta.fasta.sa"))
    )
    IndexedReferenceFasta(liftedFasta, indexFiles.toSet)

  }

  def recordsInBamFile(file: File) = {
    import htsjdk.samtools.SamReaderFactory
    import scala.collection.JavaConverters._
    val reader = SamReaderFactory.makeDefault.open(file)
    val length = reader.iterator.asScala.length
    reader.close
    length
  }

  def await[T](f: Future[T]) = Await.result(f, atMost = 180 seconds)

  def makeTestConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    val config = ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=12
      hosts.RAM=6000
      tasks.createFilePrefixForTaskId = false
      tasks.fileservice.allowDeletion = true
      """
    )
    (config, tmp)
  }

  val statsFileContent = """
    {
 "Flowcell" : "000000000-BWHDL",
 "RunNumber" : 2,
 "RunId" : "180622_M04914_0002_000000000-BWHDL",
 "ReadInfosForLanes" : [
  {
   "LaneNumber" : 1,
   "ReadInfos" : [
    {
     "Number" : 1,
     "NumCycles" : 75,
     "IsIndexedRead" : false
    },
    {
     "Number" : 1,
     "NumCycles" : 6,
     "IsIndexedRead" : true
    },
    {
     "Number" : 2,
     "NumCycles" : 75,
     "IsIndexedRead" : false
    }
   ]
  }
 ],
 "ConversionResults" : [
  {
   "LaneNumber" : 1,
   "TotalClustersRaw" : 545735,
   "TotalClustersPF" : 516918,
   "Yield" : 77537700,
   "DemuxResults" : [
    {
     "SampleId" : "GIB",
     "SampleName" : "GIB",
     "IndexMetrics" : [
      {
       "IndexSequence" : "CAGATC",
       "MismatchCounts" : {
        "0" : 43376,
        "1" : 630
       }
      }
     ],
     "NumberReads" : 44006,
     "Yield" : 6600900,
     "ReadMetrics" : [
      {
       "ReadNumber" : 1,
       "Yield" : 3300450,
       "YieldQ30" : 3237209,
       "QualityScoreSum" : 122707917,
       "TrimmedBases" : 0
      },
      {
       "ReadNumber" : 2,
       "Yield" : 3300450,
       "YieldQ30" : 3155881,
       "QualityScoreSum" : 120787860,
       "TrimmedBases" : 0
      }
     ]
    },
    {
     "SampleId" : "sample2",
     "SampleName" : "sample2",
     "IndexMetrics" : [
      {
       "IndexSequence" : "ATCACG",
       "MismatchCounts" : {
        "0" : 16696,
        "1" : 204
       }
      }
     ],
     "NumberReads" : 16900,
     "Yield" : 2535000,
     "ReadMetrics" : [
      {
       "ReadNumber" : 1,
       "Yield" : 1267500,
       "YieldQ30" : 1243254,
       "QualityScoreSum" : 47108879,
       "TrimmedBases" : 0
      },
      {
       "ReadNumber" : 2,
       "Yield" : 1267500,
       "YieldQ30" : 1237979,
       "QualityScoreSum" : 46997438,
       "TrimmedBases" : 0
      }
     ]
    }
   ],
   "Undetermined" : {
    "NumberReads" : 456012,
    "Yield" : 68401800,
    "ReadMetrics" : [
     {
      "ReadNumber" : 1,
      "Yield" : 34200900,
      "YieldQ30" : 33552579,
      "QualityScoreSum" : 1271784022,
      "TrimmedBases" : 0
     },
     {
      "ReadNumber" : 2,
      "Yield" : 34200900,
      "YieldQ30" : 32869495,
      "QualityScoreSum" : 1255791193,
      "TrimmedBases" : 0
     }
    ]
   }
  }
 ],
 "UnknownBarcodes" : [
  {
   "Lane" : 1,
   "Barcodes" : {
    "GCCAAT" : 140860,
    "CGATGT" : 61800,
    "CTTGTA" : 52020,
    "AGTCAA" : 51940,
    "AGTTCC" : 39360,
    "GCTCGG" : 26140,
    "CCGTGA" : 22140,
    "GATAGA" : 21000,
    "GACTAG" : 20040,
    "TTAGGC" : 6100,
    "CCCAAT" : 640,
    "GCTAAT" : 460,
    "GTCAAT" : 400,
    "TTTGTA" : 220,
    "TGATGT" : 220,
    "TCCCCT" : 200,
    "CGCCAA" : 200,
    "CCTCTC" : 200,
    "GCCCAA" : 160,
    "CCTCGG" : 160,
    "TCCAAT" : 140,
    "GCCTAT" : 140,
    "GCCACT" : 140,
    "TCCCAT" : 120,
    "GCCCGG" : 120,
    "GCCAAA" : 120,
    "CGTTGT" : 120,
    "CCTGTA" : 120,
    "AGTTCT" : 120,
    "AGTTAA" : 120,
    "TCCCCC" : 100,
    "GCAATA" : 100,
    "CTTGTT" : 100,
    "CTTCCC" : 100,
    "CTGTAA" : 100,
    "CGATTT" : 100,
    "AGTCAC" : 100,
    "TTACCA" : 80,
    "TCTCCT" : 80,
    "TCACCC" : 80,
    "GCCCAT" : 80,
    "GCCATT" : 80,
    "GATGTA" : 80,
    "GAAACG" : 80,
    "CTTTTC" : 80,
    "CTTTTA" : 80,
    "CTATGT" : 80,
    "CGATAT" : 80,
    "CGAGGG" : 80,
    "CCGATG" : 80,
    "CATGTA" : 80,
    "ATTCCA" : 80,
    "ATTCAA" : 80,
    "AGTTTC" : 80,
    "AGTCCA" : 80,
    "AGTCAT" : 80,
    "TGTTCC" : 60,
    "GGGGGG" : 60,
    "GCTCAA" : 60,
    "GCCATA" : 60,
    "GCCAAG" : 60,
    "GACAAT" : 60,
    "CTCTTT" : 60,
    "CTATTT" : 60,
    "CGTTCC" : 60,
    "CGAGGT" : 60,
    "CCTTTC" : 60,
    "CCTTGT" : 60,
    "CCGTGG" : 60,
    "CCCCCC" : 60,
    "CCAGAT" : 60,
    "CAGAGG" : 60,
    "ATTTCC" : 60,
    "AGTCTA" : 60,
    "AGGTTC" : 60,
    "AGACAA" : 60,
    "ACTTCC" : 60,
    "ACCAAT" : 60,
    "AAGGTA" : 60,
    "TTTCTC" : 40,
    "TTGTAA" : 40,
    "TTAGAC" : 40,
    "TGTCAA" : 40,
    "TCCTCT" : 40,
    "TCCCTC" : 40,
    "TCCCCA" : 40,
    "TCCACT" : 40,
    "TCACCT" : 40,
    "TAACGA" : 40,
    "GTCAAC" : 40,
    "GGTAGA" : 40,
    "GGAAGG" : 40,
    "GCTATC" : 40,
    "GCCGGT" : 40,
    "GCCCGT" : 40,
    "GCCAAC" : 40,
    "GAGATT" : 40,
    "GAGAGG" : 40,
    "GACGAG" : 40,
    "GAATAG" : 40,
    "CTTTCC" : 40,
    "CTTTAA" : 40,
    "CTTGGT" : 40,
    "CTTCTA" : 40,
    "CTTCCA" : 40,
    "CTTATC" : 40,
    "CTTATA" : 40,
    "CTTACC" : 40,
    "CTTAAT" : 40,
    "CTCTTC" : 40,
    "CTCGTA" : 40,
    "CGTGAG" : 40,
    "CGTCAA" : 40,
    "CGGGTA" : 40,
    "CGGGGG" : 40,
    "CGGGGA" : 40,
    "CGATGG" : 40,
    "CGATCT" : 40,
    "CGAAGT" : 40,
    "CCGTTA" : 40,
    "CCGGGA" : 40,
    "CCCTTT" : 40,
    "CCCTCT" : 40,
    "CCACTC" : 40,
    "CATCAA" : 40,
    "CAGTCA" : 40,
    "CACACC" : 40,
    "CAATAT" : 40,
    "CAAAAC" : 40,
    "ATTCCC" : 40,
    "ATGATA" : 40,
    "AGTTCG" : 40,
    "AGTACC" : 40,
    "AGGGCC" : 40,
    "AGGAAG" : 40,
    "AGCTCC" : 40,
    "AGATGT" : 40,
    "AGATCA" : 40,
    "TTTTCT" : 20,
    "TTTGGG" : 20,
    "TTTCTG" : 20,
    "TTTCCT" : 20,
    "TTTCCC" : 20,
    "TTGGTT" : 20,
    "TTGCCT" : 20,
    "TTCGCG" : 20,
    "TTCCCG" : 20,
    "TTCCCC" : 20,
    "TTCATC" : 20,
    "TTCAAT" : 20,
    "TTATTC" : 20,
    "TTATCC" : 20,
    "TTAGCT" : 20,
    "TTAGCC" : 20,
    "TTACCC" : 20,
    "TTACAG" : 20,
    "TGTAAT" : 20,
    "TGGCTT" : 20,
    "TGGCCA" : 20,
    "TGCGCC" : 20,
    "TGAGGC" : 20,
    "TGACTC" : 20,
    "TGACGA" : 20,
    "TCTTCT" : 20,
    "TCTCTG" : 20,
    "TCTCTC" : 20,
    "TCTCGG" : 20,
    "TCTCCA" : 20,
    "TCTATC" : 20,
    "TCGCCG" : 20,
    "TCCTTC" : 20,
    "TCCTAT" : 20,
    "TCCTAG" : 20,
    "TCCGCT" : 20,
    "TCCGCA" : 20,
    "TCCCAC" : 20,
    "TCCACC" : 20,
    "TCATGT" : 20,
    "TCACGA" : 20,
    "TCACCA" : 20,
    "TCACAA" : 20,
    "TCAACA" : 20,
    "TATTTC" : 20,
    "TATCGC" : 20,
    "TATCCC" : 20,
    "TAGTGG" : 20,
    "TAGGTA" : 20,
    "TAGAGG" : 20,
    "TACTAG" : 20,
    "TACCAA" : 20,
    "TAAGTT" : 20,
    "TAAGTC" : 20,
    "TAACCC" : 20,
    "TAACCA" : 20,
    "TAACAA" : 20,
    "NTTGTA" : 20,
    "NCGTGA" : 20,
    "GTTGAT" : 20,
    "GTTCCG" : 20,
    "GTTAAT" : 20,
    "GTCCCC" : 20,
    "GTAGAC" : 20,
    "GGTTCC" : 20,
    "GGCTCG" : 20,
    "GGCGAT" : 20,
    "GGAACG" : 20,
    "GCTTGG" : 20,
    "GCTGGT" : 20,
    "GCTCTT" : 20,
    "GCTCTG" : 20,
    "GCTCGT" : 20,
    "GCTCGC" : 20,
    "GCTAGG" : 20,
    "GCCTCT" : 20,
    "GCCTCA" : 20,
    "GCCGCG" : 20,
    "GCCGAT" : 20,
    "GCCCCT" : 20,
    "GCCCCC" : 20,
    "GCCATC" : 20,
    "GCCAGT" : 20,
    "GCCACC" : 20,
    "GCACGG" : 20,
    "GCACGA" : 20,
    "GATTAG" : 20,
    "GATCTA" : 20,
    "GATAGG" : 20,
    "GATAGC" : 20,
    "GAGAGA" : 20,
    "GAGACG" : 20,
    "GACTTG" : 20,
    "GACTTA" : 20,
    "GACTAT" : 20,
    "GAATAT" : 20,
    "GAAGAG" : 20,
    "GAAATT" : 20,
    "GAAAAT" : 20,
    "GAAAAA" : 20,
    "CTTTTT" : 20,
    "CTTTCT" : 20,
    "CTTGTC" : 20,
    "CTTGCA" : 20,
    "CTTGAA" : 20,
    "CTTCTT" : 20,
    "CTTCTC" : 20,
    "CTTAGG" : 20,
    "CTGCCA" : 20,
    "CTGCAG" : 20,
    "CTGCAC" : 20,
    "CTCTCC" : 20,
    "CTCTAT" : 20,
    "CTCGTC" : 20,
    "CTCCTT" : 20,
    "CTCCTC" : 20,
    "CTCCCG" : 20,
    "CTCCCC" : 20,
    "CTCCAG" : 20,
    "CTCACC" : 20,
    "CTCAAT" : 20,
    "CTATCT" : 20,
    "CTATCC" : 20,
    "CTAGTA" : 20,
    "CTAGGC" : 20,
    "CTAGCT" : 20,
    "CTAGCC" : 20,
    "CTACTC" : 20,
    "CTACCT" : 20,
    "CTACCC" : 20,
    "CTACCA" : 20,
    "CTAATA" : 20,
    "CGTGTA" : 20,
    "CGGTGG" : 20,
    "CGGGTC" : 20,
    "CGCTTT" : 20,
    "CGCCAT" : 20,
    "CGATGA" : 20,
    "CGATCA" : 20,
    "CGAGCC" : 20,
    "CGAATG" : 20,
    "CGAAAT" : 20,
    "CCTTTT" : 20,
    "CCTGTC" : 20,
    "CCTGAG" : 20,
    "CCTAAT" : 20,
    "CCGTGT" : 20,
    "CCGTGC" : 20,
    "CCGCCG" : 20,
    "CCCTGT" : 20,
    "CCCGTG" : 20,
    "CCCCTT" : 20,
    "CCCCCT" : 20,
    "CCCATT" : 20,
    "CCCACT" : 20,
    "CCATGC" : 20,
    "CCATGA" : 20,
    "CCATCT" : 20,
    "CCATAT" : 20,
    "CCATAA" : 20,
    "CCACTT" : 20,
    "CCACAA" : 20,
    "CCAACT" : 20,
    "CCAAAA" : 20,
    "CATCTA" : 20,
    "CATCAT" : 20,
    "CATCAC" : 20,
    "CAGGGG" : 20,
    "CAGGGA" : 20,
    "CAGCCC" : 20,
    "CACTTT" : 20,
    "CACTCC" : 20,
    "CACATT" : 20,
    "CAATTC" : 20,
    "CAATCC" : 20,
    "CAAAGT" : 20,
    "CAAAGG" : 20,
    "ATTGTA" : 20,
    "ATTCTA" : 20,
    "ATTCAC" : 20,
    "ATGTAT" : 20,
    "ATCTCC" : 20,
    "ATCCCA" : 20,
    "ATCAGA" : 20,
    "ATCAAC" : 20,
    "ATAGGC" : 20,
    "ATAGAC" : 20,
    "AGTTAC" : 20,
    "AGTGTC" : 20,
    "AGTGCC" : 20,
    "AGTGAA" : 20,
    "AGTCCC" : 20,
    "AGTACA" : 20,
    "AGTAAC" : 20,
    "AGGGGG" : 20,
    "AGGGCG" : 20,
    "AGGACG" : 20,
    "AGCTAA" : 20,
    "AGAGGT" : 20,
    "AGAGGG" : 20,
    "AGACGC" : 20,
    "AGACGA" : 20,
    "AGACCC" : 20,
    "ACTCCT" : 20,
    "ACCTCT" : 20,
    "ACCTAT" : 20,
    "ACCGAC" : 20,
    "ACCACT" : 20,
    "ACCAAC" : 20,
    "ACATAA" : 20,
    "ACACCC" : 20,
    "ACAAAG" : 20,
    "AATTCA" : 20,
    "AATCAA" : 20,
    "AATAAA" : 20,
    "AAGACA" : 20,
    "AACTAA" : 20,
    "AACGTG" : 20,
    "AACGTA" : 20,
    "AACGCT" : 20,
    "AACCTC" : 20,
    "AAATCC" : 20,
    "AAAGCA" : 20,
    "AAACCA" : 20,
    "AAAAAC" : 20
   }
  }
 ]
}
"""

}
