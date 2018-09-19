package org.gc.pipelines.stages

import org.gc.pipelines.model._
import org.gc.pipelines.util.Exec
import org.gc.pipelines.util

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

import tasks._
import tasks.circesupport._
import fileutils.TempFile
import scala.concurrent.Future
import java.io.File

case class PerLaneBWAAlignmentInput(
    read1: FastQ,
    read2: FastQ,
    project: Project,
    sampleId: SampleId,
    runId: RunId,
    lane: Lane,
    reference: ReferenceFasta
) extends WithSharedFiles(read1.file, read2.file, reference.file)

case class FastQPerLane(lane: Lane, read1: FastQ, read2: FastQ)

case class PerSampleBWAAlignmentInput(
    fastqs: Seq[FastQPerLane],
    project: Project,
    sampleId: SampleId,
    runId: RunId,
    reference: ReferenceFasta
) extends WithSharedFiles(fastqs.flatMap(fq =>
      List(fq.read1.file, fq.read2.file)) :+ reference.file: _*)

object BWAAlignment {

  val alignFastqPerSample =
    AsyncTask[PerSampleBWAAlignmentInput, BamWithSampleMetadata]("align-fastq",
                                                                 1) {
      case PerSampleBWAAlignmentInput(fastqs,
                                      project,
                                      sampleId,
                                      runId,
                                      reference) =>
        ce =>
          ce.withFilePrefix(Seq(project, sampleId)) {
            implicit computationEnvironment =>
              releaseResources

              def alignLane(lane: FastQPerLane) =
                alignSingleLane(
                  PerLaneBWAAlignmentInput(lane.read1,
                                           lane.read2,
                                           project,
                                           sampleId,
                                           runId,
                                           lane.lane,
                                           reference))(
                  CPUMemoryRequest(4, 12000))

              for {
                alignedLanes <- Future.sequence(fastqs.map(alignLane))
                merged <- mergeAndMarkDuplicate(
                  BamsWithSampleMetadata(project,
                                         sampleId,
                                         runId,
                                         alignedLanes.map(_.bam)))(
                  CPUMemoryRequest(4, 32000))
              } yield merged
          }
    }

  val mergeAndMarkDuplicate =
    AsyncTask[BamsWithSampleMetadata, BamWithSampleMetadata](
      "merge-markduplicate",
      1) {
      case BamsWithSampleMetadata(project, sampleId, runId, bams) =>
        implicit computationEnvironment =>
          val picardJar = extractPicardJar()

          val tempFolder =
            TempFile
              .createTempFolder(".markDuplicateTempFolder")
              .getAbsolutePath

          val tmpDuplicateMarkedBam = TempFile.createTempFile(".bam")
          val tmpMetricsFile = TempFile.createTempFile(".metrics")
          val tmpStdOut = TempFile.createTempFile(".stdout")
          val tmpStdErr = TempFile.createTempFile(".stderr")

          for {
            localBams <- Future.sequence(bams.map(_.file.file))
            result <- {

              localBams.foreach { localBam =>
                Exec.bash("buildbamindex", onError = Exec.ThrowIfNonZero)(
                  s"""java -Xmx8G -Dpicard.useLegacyParser=false -jar $picardJar BuildBamIndex \\
                 --INPUT ${localBam.getAbsolutePath} \\
                 > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)"""
                )
              }

              val inputFlags = localBams
                .map(_.getAbsolutePath)
                .mkString("--INPUT ", "--INPUT ", "")

              val bashScript = s"""
        java -Xmx32G -Dpicard.useLegacyParser=false -jar $picardJar MarkDuplicates \\
          $inputFlags \\
          --OUTPUT ${tmpDuplicateMarkedBam.getAbsolutePath} \\
          --METRICS_FILE ${tmpMetricsFile.getAbsolutePath} \\
          --OPTICAL_DUPLICATE_PIXEL_DISTANCE=250 \\
          --CREATE_INDEX=true \\
          --TMP_DIR $tempFolder \\
          > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)        
        """

              Exec.bash(logDiscriminator = "markduplicates",
                        onError = Exec.ThrowIfNonZero)(bashScript)

              val expectedBai =
                new File(
                  tmpDuplicateMarkedBam.getAbsolutePath
                    .stripSuffix(".bam") + ".bai")

              val nameStub = project + "." + sampleId + "." + runId

              for {
                _ <- SharedFile(tmpStdOut,
                                name = nameStub + ".stdout",
                                deleteFile = true)
                _ <- SharedFile(tmpStdErr,
                                name = nameStub + ".stderr",
                                deleteFile = true)
                _ <- SharedFile(expectedBai,
                                name = nameStub + ".bai",
                                deleteFile = true)
                _ <- SharedFile(tmpMetricsFile,
                                name = nameStub + ".metrics",
                                deleteFile = true)
                bam <- SharedFile(tmpDuplicateMarkedBam,
                                  name = nameStub + ".bam",
                                  deleteFile = true)
              } yield BamWithSampleMetadata(project, sampleId, runId, Bam(bam))

            }
          } yield result

    }

  val alignSingleLane =
    AsyncTask[PerLaneBWAAlignmentInput, BamWithSampleMetadataPerLane](
      "bwa-perlane",
      1) {
      case PerLaneBWAAlignmentInput(read1,
                                    read2,
                                    project,
                                    sampleId,
                                    runId,
                                    lane,
                                    reference) =>
        implicit computationEnvironment =>
          val picardJar = extractPicardJar()

          val bwaExecutable = extractBwaExecutable()

          val bwaNumberOfThreads = math.max(1, resourceAllocated.cpu - 1)

          val markAdapterMetricsFileOutput =
            TempFile.createTempFile(".markAdapterMetrics").getAbsolutePath

          val markAdapterTempFolder =
            TempFile.createTempFolder(".markAdapterTempFolder").getAbsolutePath

          val samToFastqTempFolder =
            TempFile.createTempFolder(".samToFastqTempFolder")

          val mergeBamAlignmentTempFolder =
            TempFile.createTempFolder(".mergeBamAlignmentTempFolder")

          val tmpCleanBam = TempFile.createTempFile(".bam")
          val tmpStdOut = TempFile.createTempFile(".stdout")
          val tmpStdErr = TempFile.createTempFile(".stderr")

          val readGroupName = project + "." + sampleId + "." + runId + "." + lane
          val uniqueSampleName = project + "." + sampleId

          val platformUnit = runId + "." + lane
          val sequencingCenter = "Health2030GenomeCenter"
          val runDate: String = java.time.Instant.now.toString

          for {
            read1 <- read1.file.file.map(_.getAbsolutePath)
            read2 <- read2.file.file.map(_.getAbsolutePath)
            reference <- reference.file.file
            result <- {

              Exec.bash(logDiscriminator = "bwa.index",
                        onError = Exec.ThrowIfNonZero)(
                s"$bwaExecutable index -a bwtsw $reference")

              Exec.bash(logDiscriminator = "bwa.fasta.dict",
                        onError = Exec.ThrowIfNonZero)(
                s"java -Xmx8G -Dpicard.useLegacyParser=false -jar $picardJar CreateSequenceDictionary --REFERENCE $reference"
              )

              val bashScript = s""" \\
      java -Xmx8G -Dpicard.useLegacyParser=false -jar $picardJar FastqToSam \\
        --FASTQ $read1 \\
        --FASTQ2 $read2 \\
        --OUTPUT /dev/stdout \\
        --QUIET true \\
        --SORT_ORDER queryname \\
        --COMPRESSION_LEVEL 0 \\
        --READ_GROUP_NAME $readGroupName \\
        --SAMPLE_NAME $uniqueSampleName \\
        --LIBRARY_NAME $uniqueSampleName \\
        --PLATFORM_UNIT $platformUnit \\
        --PLATFORM illumina \\
        --SEQUENCING_CENTER  $sequencingCenter \\
        --RUN_DATE $runDate 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2) | \\
      \\
     java -Xmx8G -Dpicard.useLegacyParser=false -jar $picardJar MarkIlluminaAdapters \\
       --INPUT /dev/stdin \\
       --OUTPUT /dev/stdout \\
       --QUIET true \\
       --METRICS $markAdapterMetricsFileOutput \\
       --TMP_DIR $markAdapterTempFolder 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2) | \\
     \\
     java -Xmx8G -Dpicard.useLegacyParser=false -jar $picardJar SamToFastq \\
       --INPUT /dev/stdin \\
       --FASTQ /dev/stdout \\
       --QUIET true \\
       --CLIPPING_ATTRIBUTE XT \\
       --CLIPPING_ACTION 2 \\
       --INTERLEAVE true \\
       --INCLUDE_NON_PF_READS true \\
       --TMP_DIR $samToFastqTempFolder 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2) | \\
     \\
     $bwaExecutable mem -M -t $bwaNumberOfThreads -p $reference /dev/stdin 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2) | \\
     \\
     java -Xmx16G -Dpicard.useLegacyParser=false -jar $picardJar MergeBamAlignment \\
       --REFERENCE_SEQUENCE $reference \\
       --UNMAPPED_BAM <(
           java -Xmx8G -Dpicard.useLegacyParser=false -jar $picardJar FastqToSam \\
             --FASTQ $read1 \\
             --FASTQ2 $read2 \\
             --OUTPUT /dev/stdout \\
             --QUIET true \\
             --SORT_ORDER queryname \\
             --COMPRESSION_LEVEL 0 \\
             --READ_GROUP_NAME $readGroupName \\
             --SAMPLE_NAME $uniqueSampleName \\
             --LIBRARY_NAME $uniqueSampleName \\
             --PLATFORM_UNIT $platformUnit \\
             --PLATFORM illumina \\
             --SEQUENCING_CENTER  $sequencingCenter \\
             --RUN_DATE $runDate
         ) \\
       --ALIGNED_BAM /dev/stdin \\
       --OUTPUT ${tmpCleanBam.getAbsolutePath} \\
       --CREATE_INDEX true \\
       --ADD_MATE_CIGAR true \\
       --CLIP_ADAPTERS false \\
       --CLIP_OVERLAPPING_READS true \\
       --INCLUDE_SECONDARY_ALIGNMENTS true \\
       --MAX_INSERTIONS_OR_DELETIONS -1 \\
       --PRIMARY_ALIGNMENT_STRATEGY MostDistant \\
       --ATTRIBUTES_TO_RETAIN XS \\
       --TMP_DIR $mergeBamAlignmentTempFolder \\
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)        
      """

              Exec.bash(logDiscriminator = "bwa.pipes",
                        onError = Exec.ThrowIfNonZero)(bashScript)

              val nameStub = readGroupName

              for {
                _ <- SharedFile(tmpStdOut, name = nameStub + ".stdout")
                _ <- SharedFile(tmpStdErr, name = nameStub + ".stderr")
                bam <- SharedFile(tmpCleanBam, name = nameStub + ".bam")
              } yield
                BamWithSampleMetadataPerLane(project,
                                             sampleId,
                                             runId,
                                             lane,
                                             Bam(bam))
            }
          } yield result

    }

  private def extractPicardJar(): String =
    fileutils.TempFile
      .getExecutableFromJar("/bin/picard_2.8.14.jar", "picard_2.8.14.jar")
      .getAbsolutePath

  private def extractBwaExecutable(): String = {
    val resourceName =
      if (util.isMac) "/bin/bwa_0.7.17-r1188_mac"
      else if (util.isLinux) "/bin/bwa_0.7.17-r1188_linux64"
      else
        throw new RuntimeException(
          "Unknown OS: " + System.getProperty("os.name"))
    fileutils.TempFile
      .getExecutableFromJar(resourceName, "bwa_0.7.17-r1188")
      .getAbsolutePath
  }

}

object PerLaneBWAAlignmentInput {
  implicit val encoder: Encoder[PerLaneBWAAlignmentInput] =
    deriveEncoder[PerLaneBWAAlignmentInput]
  implicit val decoder: Decoder[PerLaneBWAAlignmentInput] =
    deriveDecoder[PerLaneBWAAlignmentInput]
}

object PerSampleBWAAlignmentInput {
  implicit val encoder: Encoder[PerSampleBWAAlignmentInput] =
    deriveEncoder[PerSampleBWAAlignmentInput]
  implicit val decoder: Decoder[PerSampleBWAAlignmentInput] =
    deriveDecoder[PerSampleBWAAlignmentInput]
}

object FastQPerLane {
  implicit val encoder: Encoder[FastQPerLane] =
    deriveEncoder[FastQPerLane]
  implicit val decoder: Decoder[FastQPerLane] =
    deriveDecoder[FastQPerLane]
}
