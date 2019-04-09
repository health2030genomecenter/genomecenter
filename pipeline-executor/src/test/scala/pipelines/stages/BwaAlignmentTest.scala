package org.gc.pipelines.stages

import org.scalatest._

import tasks._
import java.io.File

import org.gc.pipelines.model._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import org.gc.pipelines.util.{FastQHelpers, StableSet}

class BwaAlignmentTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {

  test("Bwa alignment stage should produce a clean bam file") {
    new Fixture {

      val result = withTaskSystem(testConfig) { implicit ts =>
        val indexedFasta = fetchIndexedReference(referenceFile)

        val input =
          PerLaneBWAAlignmentInput(
            read1 =
              FastQ(await(SharedFile(fastq1, "fastq1.gz")), 10000L, Some(100)),
            read2 =
              FastQ(await(SharedFile(fastq2, "fastq2.gz")), 10000L, Some(100)),
            project = project,
            sampleId = sampleId,
            runId = runId,
            lane = lane,
            partition = PartitionId(0),
            reference = indexedFasta,
            umi = None,
            interval = None
          )

        val future =
          BWAAlignment.alignSingleLane(input)(ResourceRequest(1, 500))
        val bamWithSampleMetadata = await(future)
        val bamFile = await(bamWithSampleMetadata.bam.file.file)
        (bamWithSampleMetadata, bamFile)
      }

      val (bamWithMetadata, localBam) = result.get
      bamWithMetadata.project shouldBe project
      bamWithMetadata.runId shouldBe runId
      localBam.canRead shouldBe true
      new File(localBam.getParentFile, localBam.getName + ".stderr").canRead shouldBe true

      recordsInBamFile(localBam) shouldBe 10000

      getSortOrder(localBam) shouldBe "queryname"

      takeRecordsInBamFile(localBam, 100).foreach { record =>
        record.getReadUnmappedFlag shouldBe false
        record.getReferenceName.take(5) shouldBe "chr19"
      }

    }
  }

  test(
    "Bwa alignment stage should produce a clean bam file and process UMIs if present") {
    new Fixture {

      val result = withTaskSystem(testConfig) { implicit ts =>
        val indexedFasta = fetchIndexedReference(referenceFile)

        val input =
          PerLaneBWAAlignmentInput(
            read1 = FastQ(await(SharedFile(fastq1, "fastq1.gz")), 10000L, None),
            read2 = FastQ(await(SharedFile(fastq2, "fastq2.gz")), 10000L, None),
            project = project,
            sampleId = sampleId,
            runId = runId,
            lane = lane,
            partition = PartitionId(0),
            reference = indexedFasta,
            umi =
              Some(FastQ(await(SharedFile(fastq2, "fastq2.gz")), 10000L, None)),
            interval = None
          )

        val future =
          BWAAlignment.alignSingleLane(input)(ResourceRequest(1, 500))
        val bamWithSampleMetadata = await(future)
        val bamFile = await(bamWithSampleMetadata.bam.file.file)
        (bamWithSampleMetadata, bamFile)
      }

      val (bamWithMetadata, localBam) = result.get
      bamWithMetadata.project shouldBe project
      bamWithMetadata.runId shouldBe runId
      localBam.canRead shouldBe true
      new File(localBam.getParentFile, localBam.getName + ".stderr").canRead shouldBe true

      recordsInBamFile(localBam) shouldBe 10000

      getSortOrder(localBam) shouldBe "queryname"

      takeRecordsInBamFile(localBam, 100).foreach { record =>
        record.getReadUnmappedFlag shouldBe false
        record.getReferenceName.take(5) shouldBe "chr19"
        record.getAttribute("OX").toString.size shouldBe 151
      }

    }
  }

  test(
    "Bwa alignment stage should produce a clean bam file and process UMIs if present, within an interval") {
    new Fixture {

      val result = withTaskSystem(testConfig) { implicit ts =>
        val indexedFasta = fetchIndexedReference(referenceFile)

        val input =
          PerLaneBWAAlignmentInput(
            read1 = FastQ(await(SharedFile(fastq1, "fastq1.gz")), 10000L, None),
            read2 = FastQ(await(SharedFile(fastq2, "fastq2.gz")), 10000L, None),
            project = project,
            sampleId = sampleId,
            runId = runId,
            lane = lane,
            partition = PartitionId(0),
            reference = indexedFasta,
            umi =
              Some(FastQ(await(SharedFile(fastq2, "fastq2.gz")), 10000L, None)),
            interval = Some(
              IntervalTriplet(read1Intervals.head,
                              read2Intervals.head,
                              Some(read2Intervals.head)))
          )

        val future =
          BWAAlignment.alignSingleLane(input)(ResourceRequest(1, 500))
        val bamWithSampleMetadata = await(future)
        val bamFile = await(bamWithSampleMetadata.bam.file.file)
        (bamWithSampleMetadata, bamFile)
      }

      val (bamWithMetadata, localBam) = result.get
      bamWithMetadata.project shouldBe project
      bamWithMetadata.runId shouldBe runId
      localBam.canRead shouldBe true
      new File(localBam.getParentFile, localBam.getName + ".stderr").canRead shouldBe true

      recordsInBamFile(localBam) shouldBe 1998

      getSortOrder(localBam) shouldBe "queryname"

      takeRecordsInBamFile(localBam, 100).foreach { record =>
        record.getReadUnmappedFlag shouldBe false
        record.getReferenceName.take(5) shouldBe "chr19"
        record.getAttribute("OX").toString.size shouldBe 151
      }

    }
  }
  test("Bwa alignment stage should produce a clean bam file within an interval") {
    new Fixture {

      val result = withTaskSystem(testConfig) { implicit ts =>
        val indexedFasta = fetchIndexedReference(referenceFile)

        val input =
          PerLaneBWAAlignmentInput(
            read1 = FastQ(await(SharedFile(fastq1, "fastq1.gz")), 10000L, None),
            read2 = FastQ(await(SharedFile(fastq2, "fastq2.gz")), 10000L, None),
            project = project,
            sampleId = sampleId,
            runId = runId,
            lane = lane,
            partition = PartitionId(0),
            reference = indexedFasta,
            umi = None,
            interval = Some(
              IntervalTriplet(read1Intervals.head,
                              read2Intervals.head,
                              Some(read2Intervals.head)))
          )

        val future =
          BWAAlignment.alignSingleLane(input)(ResourceRequest(1, 500))
        val bamWithSampleMetadata = await(future)
        val bamFile = await(bamWithSampleMetadata.bam.file.file)
        (bamWithSampleMetadata, bamFile)
      }

      val (bamWithMetadata, localBam) = result.get
      bamWithMetadata.project shouldBe project
      bamWithMetadata.runId shouldBe runId
      localBam.canRead shouldBe true
      new File(localBam.getParentFile, localBam.getName + ".stderr").canRead shouldBe true

      recordsInBamFile(localBam) shouldBe 1998

      getSortOrder(localBam) shouldBe "queryname"

      takeRecordsInBamFile(localBam, 100).foreach { record =>
        record.getReadUnmappedFlag shouldBe false
        record.getReferenceName.take(5) shouldBe "chr19"
      }

    }
  }

  test("Bwa alignment stage should scatter correctly", org.gc.pipelines.Only) {
    new Fixture {

      val result = withTaskSystem(testConfig) { implicit ts =>
        val indexedFasta = fetchIndexedReference(referenceFile)

        val input =
          PerSampleBWAAlignmentInput(
            fastqs = StableSet(
              FastQPerLane(
                runId,
                lane,
                FastQ(await(SharedFile(fastq1, "fastq1.gz")), 10000L, None),
                FastQ(await(SharedFile(fastq2, "fastq2.gz")), 10000L, None),
                Some(
                  FastQ(await(SharedFile(fastq2, "fastq2.gz")), 10000L, None)),
                PartitionId(2)
              )),
            project = project,
            sampleId = sampleId,
            reference = indexedFasta
          )

        val future =
          BWAAlignment.alignFastqPerSample(input)(ResourceRequest(1, 500))
        val bamWithSampleMetadata = await(future)
        val bamFiles =
          await(
            Future.traverse(bamWithSampleMetadata.alignedLanes.toSeq)(
              _.bam.file.file))
        (bamWithSampleMetadata, bamFiles)
      }

      val (_, localBams) = result.get
      localBams.forall(_.canRead) shouldBe true
      localBams.foreach { localBam =>
        new File(localBam.getParentFile, localBam.getName + ".stderr").canRead shouldBe true

        getSortOrder(localBam) shouldBe "queryname"

        takeRecordsInBamFile(localBam, 100).foreach { record =>
          record.getReadUnmappedFlag shouldBe false
          record.getReferenceName.take(5) shouldBe "chr19"
          record.getAttribute("OX").toString.size shouldBe 151
        }
      }

      localBams.map(localBam => recordsInBamFile(localBam)).sum shouldBe 10000

    }
  }

  trait Fixture {

    val project = Project("someProject")
    val sampleId = SampleId("someSampleId")
    val runId = RunId("someRunId")
    val lane = Lane(1)

    val fastq1 = new File(
      getClass.getResource("/tutorial_8017/papa.read1.bgzip").getFile)
    val fastq2 = new File(
      getClass.getResource("/tutorial_8017/papa.read2.bgzip").getFile)
    val referenceFile = new File(
      getClass
        .getResource("/tutorial_8017/chr19_chr19_KI270866v1_alt.fasta")
        .getFile)

    val read1Intervals = FastQHelpers.indexFastQSplits(fastq1, 999L)
    val read2Intervals = FastQHelpers.indexFastQSplits(fastq2, 999L)

    val (testConfig, basePath) = makeTestConfig
  }
}
