package org.gc.pipelines.stages

import org.gc.pipelines.model._
import org.gc.pipelines.util.Exec

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

import tasks._
import tasks.circesupport._
import fileutils.TempFile

case class PerLaneAlignmentInput(
    read1: FastQ,
    read2: FastQ,
    project: Project,
    sampleId: SampleId,
    runId: RunId,
    lane: Lane,
    reference: ReferenceFasta
)

object Alignment {
  val alignSingleLane =
    AsyncTask[PerLaneAlignmentInput, BamWithSampleMetadataPerLane](
      "alignfastqs-perlane",
      1) {
      case PerLaneAlignmentInput(read1,
                                 read2,
                                 project,
                                 sampleId,
                                 runId,
                                 lane,
                                 reference) =>
        implicit computationEnvironment =>
          val picardJar: String =
            fileutils.TempFile
              .getExecutableFromJar("/picard.jar")
              .getAbsolutePath

          val bwaExecutable: String =
            fileutils.TempFile.getExecutableFromJar("/bwa").getAbsolutePath

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

              Exec.bash(logDiscriminator = "bwa.index")(
                s"$bwaExecutable index -a bwtsw $reference")

              val bashScript = s"""
      java -Xmx8G -jar $picardJar FastqToSam \
        FASTQ=$read1 \ 
        FASTQ2=$read2 \ 
        OUTPUT=/dev/stdout \
        QUIET=true \
        SORT_ORDER=queryname \
        COMPRESSION=0 \
        READ_GROUP_NAME=$readGroupName \ 
        SAMPLE_NAME=$uniqueSampleName \ 
        LIBRARY_NAME=$uniqueSampleName \ 
        PLATFORM_UNIT=$platformUnit \ 
        PLATFORM=illumina \ 
        SEQUENCING_CENTER= $sequencingCenter \ 
        RUN_DATE=$runDate | \
      \
      java -Xmx8G -jar $picardJar MarkIlluminaAdapters \
        INPUT=/dev/stdin \
        OUTPUT=/dev/stdout \
        QUIET=true \
        METRICS=$markAdapterMetricsFileOutput \ 
        TMP_DIR=$markAdapterTempFolder | \
      \
      java -Xmx8G -jar $picardJar SamToFastq \
        INPUT=/dev/stdin \
        FASTQ=/dev/stdout \
        QUIET=true \
        CLIPPING_ATTRIBUTE=XT \
        CLIPPING_ACTION=2 \
        INTERLEAVE=true \ 
        INCLUDE_NON_PF_READS=true \
        TMP_DIR=$samToFastqTempFolder | \
      \
      $bwaExecutable mem -M -t $bwaNumberOfThreads -p $reference /dev/stdin | \
      \
      java -Xmx16G -jar $picardJar MergeBamAlignment \
        REFERENCE_SEQUENCE=$reference \ 
        UNMAPPED_BAM=<(
            java -Xmx8G -jar $picardJar FastqToSam \
              FASTQ=$read1 \ 
              FASTQ2=$read2 \ 
              OUTPUT=/dev/stdount \
              QUIET=true \
              SORT_ORDER=queryname \
              COMPRESSION=0 \
              READ_GROUP_NAME=$readGroupName \ 
              SAMPLE_NAME=$uniqueSampleName \ 
              LIBRARY_NAME=$uniqueSampleName \ 
              PLATFORM_UNIT=$platformUnit \ 
              PLATFORM=illumina \ 
              SEQUENCING_CENTER= $sequencingCenter \ 
              RUN_DATE=$runDate
          ) \ 
        ALIGNED_BAM=/dev/stdin \ 
        OUTPUT=${tmpCleanBam.getAbsolutePath} \
        CREATE_INDEX=true \ 
        ADD_MATE_CIGAR=true \ 
        CLIP_ADAPTERS=false \ 
        CLIP_OVERLAPPING_READS=true \ #soft-clips ends so mates do not extend past each other
        INCLUDE_SECONDARY_ALIGNMENTS=true \
        MAX_INSERTIONS_OR_DELETIONS=-1 \ #changed to allow any number of insertions or deletions
        PRIMARY_ALIGNMENT_STRATEGY=MostDistant \ #changed from default BestMapq
        ATTRIBUTES_TO_RETAIN=XS \ 
        TMP_DIR=$mergeBamAlignmentTempFolder 1> ${tmpStdOut.getAbsolutePath} 2> ${tmpStdErr.getAbsolutePath}
      """

              Exec.bash(logDiscriminator = "alignment.pipes")(bashScript)

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

}

object PerLaneAlignmentInput {
  implicit val encoder: Encoder[PerLaneAlignmentInput] =
    deriveEncoder[PerLaneAlignmentInput]
  implicit val decoder: Decoder[PerLaneAlignmentInput] =
    deriveDecoder[PerLaneAlignmentInput]
}
